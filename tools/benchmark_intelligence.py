"""Reproducible offline load test. No real platform or external network is used.

python tools/benchmark_intelligence.py --directory .test-data/scale --seed-only
python tools/benchmark_intelligence.py --directory .test-data/scale --seconds 1800 --rate 50
"""
import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp
from adapters.onebot_fields import register_onebot_fields
from core import APP_PATHS, Bot, Envelope
from shared.paths import RuntimePaths

from tiffany_plugin_repeater import SERVICE, RepeaterConfig, install
from tiffany_plugin_repeater.context import packed
from tiffany_plugin_repeater.models import Content, Segment, keywords
from tiffany_plugin_repeater.storage import Store

ADAPTER = "onebot:benchmark"
ACCOUNT = "9000"
NAMESPACE = packed([ADAPTER, ACCOUNT])
SQL_PROFILE = defaultdict(lambda: {"count": 0, "seconds": 0., "max_ms": 0.})


class ErrorCounter(logging.Handler):
    def __init__(self):
        super().__init__(logging.ERROR)
        self.messages = []

    def emit(self, record):
        if len(self.messages) < 20:
            self.messages.append(record.getMessage())


class ProfilingConnection(sqlite3.Connection):
    def execute(self, sql, parameters=(), /):
        started = time.perf_counter()
        try:
            return super().execute(sql, parameters)
        finally:
            item = SQL_PROFILE[" ".join(sql.split())[:180]]
            elapsed = time.perf_counter() - started
            item["count"] += 1
            item["seconds"] += elapsed
            item["max_ms"] = max(item["max_ms"], elapsed * 1000)


def group_key(group):
    return packed([ADAPTER, ACCOUNT, str(group)])


def expressions():
    contents = []
    for topic in range(8):
        contents.extend([Content.text(f"需求{topic}今天又没做完"), Content.text("今天又要加班" if topic % 2 else "老板今天还不让走"),
                         Content.text(("建议老板自己干", "什么时候才能辞职", "明天还得继续打卡")[topic % 3]),
                         Content((Segment("face", 100 + topic),))])
    return contents


async def seed(directory, groups, evidence):
    config = RepeaterConfig()
    store = Store(directory / "storage" / "repeater", config)
    await store.open()
    try:
        existing = await store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0])
        per_group = evidence // groups
        metadata_path = directory / "seed.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text("utf-8"))
            if metadata != {"groups": groups, "evidence": per_group * groups}:
                raise ValueError("existing benchmark uses different seed parameters")
            print(json.dumps({"seed_reused": existing}), flush=True)
            return metadata["evidence"]
        completed_groups = await store.read(lambda db: {r[0]: r[1] for r in db.execute("SELECT group_key,COUNT(*) FROM learning_evidence GROUP BY group_key")})
        pool = expressions()
        now = time.time()
        for group in range(groups):
            key = group_key(group)
            if completed_groups.get(key) == per_group:
                continue
            if completed_groups.get(key):
                raise ValueError("partial group in seed; use a new benchmark directory")
            rows, links, messages = [], [], []
            history = []
            for i in range(per_group):
                content = pool[i % len(pool)]
                identity = group * per_group + i + 1
                created = now - (per_group - i) * 5
                event_key = packed([key, str(i + 1)])
                actor = str(100 + i % 11)
                rows.append((identity, key, event_key, str(i + 1), actor, created, content.key, packed(history), None, 0, 1))
                messages.append((identity, key, actor, event_key, created, content.key, content.dump(), content.words, content.media))
                for depth in (1, 2, 3):
                    if len(history) >= depth:
                        context = [r["content_key"] for r in history[-depth:]]
                        links.append((identity, key, packed(context), depth, context[-1], content.key, actor, int(created // 300), .5, created))
                history = (history + [{"event_key": event_key, "content_key": content.key, "created": created}])[-3:]
            def write(db, *, key=key, group=group, history=history, rows=rows, links=links, messages=messages):
                with db:
                    db.execute("INSERT INTO groups(key,namespace,target,enabled) VALUES(?,?,?,1)", (key, NAMESPACE, str(group)))
                    db.execute("INSERT INTO learning_state(group_key,tail) VALUES(?,?)", (key, packed(history)))
                    db.execute("INSERT INTO learning_sizes(group_key,evidence_count) VALUES(?,?)", (key, per_group))
                    for content in {c.key: c for c in pool}.values():
                        store._utterance(db, key, content, now)
                    db.executemany("INSERT INTO learning_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
                    db.executemany("INSERT INTO evidence_links VALUES(?,?,?,?,?,?,?,?,?,?)", links)
                    db.executemany("INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?)", messages)
                    db.execute("""INSERT INTO evidence_support SELECT group_key,context,answer,actor,time_bucket,
                      MAX(base_weight),COUNT(*),MAX(created) FROM evidence_links WHERE group_key=?
                      GROUP BY group_key,context,answer,actor,time_bucket""", (key,))
                    db.execute("""INSERT INTO context_counts SELECT group_key,context,json_array_length(context),
                      json_extract(context,'$[#-1]'),answer,SUM(raw_count),SUM(weight),COUNT(*),MAX(updated)
                      FROM evidence_support WHERE group_key=? GROUP BY group_key,context,answer""", (key,))
                    db.execute("INSERT INTO reply_edges SELECT group_key,prompt,answer,raw_count,effective_count,independent_count,updated FROM context_counts WHERE group_key=? AND depth=1", (key,))
                    db.execute("""INSERT INTO variants SELECT l.group_key,p.words,CASE WHEN a.media THEN a.content_key ELSE a.words END,
                      a.content_key,a.payload,a.words,a.media,COUNT(*),MAX(l.created)
                      FROM evidence_links l JOIN utterances p ON p.group_key=l.group_key AND p.content_key=l.prompt
                      JOIN utterances a ON a.group_key=l.group_key AND a.content_key=l.answer WHERE l.group_key=? AND l.depth=1
                      GROUP BY l.group_key,p.words,CASE WHEN a.media THEN a.content_key ELSE a.words END,a.content_key""", (key,))
                    db.execute("INSERT INTO relations SELECT group_key,prompt,signature,SUM(count),MAX(updated) FROM variants WHERE group_key=? GROUP BY group_key,prompt,signature", (key,))
                    db.execute("INSERT INTO graph_dirty SELECT group_key,prompt,1 FROM reply_edges WHERE group_key=? GROUP BY prompt", (key,))
            await store.call(write)
            if (group + 1) % 10 == 0 or group + 1 == groups:
                print(json.dumps({"seed_groups": group + 1, "seed_evidence": (group + 1) * per_group}), flush=True)
        metadata_path.write_text(json.dumps({"groups": groups, "evidence": per_group * groups}), encoding="utf-8")
        return per_group * groups
    finally:
        await store.close()


class PlatformStub:
    running = True
    connection_id = "benchmark:continuous"

    def __init__(self):
        self.sequence = 10_000_000
        self.calls = 0

    async def call(self, action, params):
        self.calls += 1
        self.sequence += 1
        return SimpleNamespace(data={"message_id": self.sequence})

    async def reply(self, ctx, text):
        return await self.call("send_group_msg", {"message": text})


def percentile(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p))] if values else 0


async def run(args):
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    seeded = await seed(directory, args.groups, args.evidence)
    if args.seed_only:
        return
    bot = Bot()
    bot.service(APP_PATHS, RuntimePaths(directory))
    register_onebot_fields(bot, "napcat")
    config = RepeaterConfig(delay_min_seconds=0, delay_max_seconds=0, dashboard_enabled=True,
                            dashboard_port=0, dashboard_token="benchmark-local-token-only")
    install(bot.scope("plugins.repeater"), config)
    client = PlatformStub()
    latencies, queue_samples, errors = [], [], []
    event_latencies = defaultdict(list)
    pending = set()
    expected = 0
    source_digest = hashlib.sha256(b"".join(p.read_bytes() for p in sorted((Path(__file__).resolve().parents[1] / "tiffany_plugin_repeater").glob("*.py")))).hexdigest()
    await bot.start()
    service = bot.services.get(SERVICE)
    pool = expressions()
    # Initialize lexical resources before timed load, on a worker thread.
    await asyncio.to_thread(lambda: [keywords(c.plain) for c in pool if c.plain])
    # Give the fixed corpus >=20 distinct text antecedents in every group so
    # timed load exercises real graph computation, including its warm-up gate.
    warmup_marker = directory / "graph-warmup.json"
    if not warmup_marker.exists():
        for group in range(args.groups):
            for i in range(16):
                text = f"规模测试的独立前文{i}" if i % 2 == 0 else f"规模测试的独立回答{i}"
                event = SimpleNamespace(group_key=group_key(group), namespace=NAMESPACE,
                                        event_key=f"warmup:{group}:{i}", message_id=f"warmup:{i}", author="warmup", reply_to=None)
                history, _ = await service.store.history(event.group_key, time.time())
                await service.store.observe(event, Content.text(text), history, time.time())
        warmup_marker.write_text(json.dumps({"events": args.groups * 16}), encoding="utf-8")
    before = await service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0])
    base_sequence = int(time.time() * 1_000_000)
    error_counter = ErrorCounter()
    logging.getLogger().addHandler(error_counter)
    started = time.perf_counter()
    dashboard_counts = {"ok": 0, "busy": 0, "errors": 0}
    stopping = asyncio.Event()

    async def dashboard_load():
        base = f"http://127.0.0.1:{service.dashboard.port}"
        async with aiohttp.ClientSession(headers={"Authorization": "Bearer " + config.dashboard_token}) as session:
            index = 0
            while not stopping.is_set():
                view = ("evidence", "decisions", "feedback", "neighbors", "overview")[index % 5]
                params = {"group": group_key(index % args.groups), "limit": "20"}
                try:
                    async with session.get(base + "/api/" + view, params=params) as response:
                        await response.read()
                        dashboard_counts["ok" if response.status == 200 else "busy" if response.status == 503 else "errors"] += 1
                except Exception as exc:
                    errors.append(repr(exc))
                index += 1
                try:
                    await asyncio.wait_for(stopping.wait(), timeout=2)
                except TimeoutError:
                    pass

    async def emit(index, due):
        nonlocal expected
        group = index % args.groups
        raw = {"post_type": "message", "message_type": "group", "self_id": int(ACCOUNT), "group_id": group,
               "user_id": 100 + index % 11, "message_id": base_sequence + index,
               "message": [{"type": "text", "data": {"text": pool[(index // args.groups) % len(pool)].plain}}]}
        content = pool[(index // args.groups) % len(pool)]
        if content.media:
            raw["message"] = [{"type": "face", "data": {"id": content.segments[0].value}}]
        receipts = service.sender.receipts.get(group_key(group), ())
        if index % 20 == 0 and receipts:
            raw = {"post_type": "notice", "notice_type": "group_msg_emoji_like", "self_id": int(ACCOUNT), "group_id": group,
                   "user_id": 100 + index % 11, "message_id": receipts[-1][0], "is_add": True,
                   "likes": [{"emoji_id": str(index % 4), "count": 1}]}
        elif index % 20 == 1 and receipts:
            raw["message"] = [{"type": "reply", "data": {"id": receipts[-1][0]}}, {"type": "text", "data": {"text": "你可真是个人才"}}]
        else:
            expected += 1
        try:
            await bot.emit(Envelope("napcat", raw, client=client, kind=raw["post_type"], adapter_id=ADAPTER, session_id=str(group)))
        except Exception as exc:
            errors.append(repr(exc))
        elapsed = time.perf_counter() - due
        latencies.append(elapsed)
        event_type = "notice" if raw["post_type"] == "notice" else "quote" if raw["message"][0]["type"] == "reply" else "media" if content.media else "text"
        event_latencies[event_type].append(elapsed)

    dashboard_task = asyncio.create_task(dashboard_load())
    try:
        total = int(args.seconds * args.rate)
        for i in range(total):
            due = started + i / args.rate
            wait = due - time.perf_counter()
            if wait > 0:
                await asyncio.sleep(wait)
            task = asyncio.create_task(emit(i, due))
            pending.add(task)
            task.add_done_callback(pending.discard)
            if i % max(1, int(args.rate * 10)) == 0:
                queue_samples.append(len(pending))
            if i and i % max(1, int(args.rate * 60)) == 0:
                print(json.dumps({"elapsed": round(time.perf_counter() - started), "completed": len(latencies), "pending": len(pending), "p95_ms": round(percentile(latencies, .95) * 1000, 2)}), flush=True)
        await asyncio.gather(*tuple(pending))
        if service.pending:
            await asyncio.gather(*tuple(service.pending.values()), return_exceptions=True)
        stopping.set()
        await dashboard_task
        while await service.store.settle_feedback(time.time() + config.feedback_window_seconds):
            pass
        after = await service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0])
        duplicate_events = await service.store.read(lambda db: db.execute("SELECT COUNT(*)-COUNT(DISTINCT event_key) FROM learning_evidence").fetchone()[0])
        duplicate_receipts = await service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM (SELECT group_key,message_id,COUNT(*) n FROM delivery_receipts WHERE message_id IS NOT NULL GROUP BY group_key,message_id HAVING n>1)").fetchone()[0])
        settlements = await service.store.read(lambda db: {
            "episodes": db.execute("SELECT COUNT(*),COALESCE(SUM(reward),0) FROM reply_episodes WHERE settled=1 AND observable=1 AND status='sent' AND mode='normal'").fetchone(),
            "stats": db.execute("SELECT COALESCE(SUM(exposures),0),COALESCE(SUM(reward),0) FROM feedback_stats").fetchone(),
        })
        settlements = {name: list(values) for name, values in settlements.items()}
        report = {"hardware": {"platform": platform.platform(), "processor": platform.processor(), "logical_cpus": os.cpu_count(), "python": sys.version, "sqlite": sqlite3.sqlite_version},
                  "source_sha256": source_digest, "groups": args.groups, "seed_evidence": seeded, "seconds": args.seconds, "rate": args.rate,
                  "initial_evidence": before,
                  "events": len(latencies), "duration": time.perf_counter() - started, "p50_ms": percentile(latencies, .5) * 1000,
                  "p95_ms": percentile(latencies, .95) * 1000, "p99_ms": percentile(latencies, .99) * 1000,
                  "queue_samples": queue_samples, "expected_new_evidence": expected, "actual_new_evidence": after - before,
                  "duplicate_events": duplicate_events, "duplicate_receipts": duplicate_receipts, "platform_calls": client.calls,
                  "dashboard": dashboard_counts, "metrics": dict(service.store.metrics), "errors": errors[:20],
                  "runtime_errors": error_counter.messages, "settlement": settlements,
                  "event_types": {name: {"count": len(values), "p95_ms": percentile(values, .95) * 1000,
                                          "p99_ms": percentile(values, .99) * 1000} for name, values in event_latencies.items()}}
        report["sql_profile"] = sorted(({"sql": sql, **values} for sql, values in SQL_PROFILE.items()), key=lambda r: -r["seconds"])[:30]
        report["database_bytes"] = sum(p.stat().st_size for p in service.store.path.parent.glob("repeater.sqlite3*"))
        failures = ("storage_busy", "retrieval_failures", "feedback_capture_failures", "intelligence_background_failures")
        report["passed"] = (report["event_types"].get("text", {}).get("p95_ms", float("inf")) <= 200
                            and after - before == expected
                            and not duplicate_events and not duplicate_receipts and not errors
                            and not error_counter.messages and not dashboard_counts["errors"]
                            and not any(service.store.metrics[name] for name in failures)
                            and settlements["episodes"][0] == settlements["stats"][0]
                            and abs(settlements["episodes"][1] - settlements["stats"][1]) < 1e-6
                            and max(queue_samples[-6:], default=0) < 100)
        destination = Path(args.report) if args.report else directory / "report.json"
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"report": str(destination.resolve()), "passed": report["passed"], "p95_ms": report["p95_ms"], "events": len(latencies)}), flush=True)
    finally:
        logging.getLogger().removeHandler(error_counter)
        stopping.set()
        await dashboard_task
        await bot.stop("abort")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", default=".test-data/scale")
    parser.add_argument("--groups", type=int, default=200)
    parser.add_argument("--evidence", type=int, default=2_000_000)
    parser.add_argument("--seconds", type=float, default=1800)
    parser.add_argument("--rate", type=float, default=50)
    parser.add_argument("--report")
    parser.add_argument("--seed-only", action="store_true")
    parser.add_argument("--profile-sql", action="store_true")
    arguments = parser.parse_args()
    if arguments.groups < 1 or arguments.evidence < arguments.groups or arguments.seconds <= 0 or arguments.rate <= 0:
        parser.error("groups, evidence, seconds and rate must be positive")
    if arguments.profile_sql:
        original_connect = sqlite3.connect
        def profiled_connect(*args, **kwargs):
            kwargs.setdefault("factory", ProfilingConnection)
            return original_connect(*args, **kwargs)
        sqlite3.connect = profiled_connect
    asyncio.run(run(arguments))
