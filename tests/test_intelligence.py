"""Behavioral checks for facts, inference, feedback and real host integration."""
import random
import shutil
import sqlite3
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from core import Envelope

from tests import test_native as native
from tests.test_native import FakeOneBot
from tiffany_plugin_repeater.config import RepeaterConfig
from tiffany_plugin_repeater.context import contextual_distribution, lexical_similarity
from tiffany_plugin_repeater.engine import Engine
from tiffany_plugin_repeater.feedback import FeedbackCollector, FeedbackObservation
from tiffany_plugin_repeater.graph import GraphWorker, build_neighbors
from tiffany_plugin_repeater.models import (
    Content,
    DeliveryReceipt,
    ReplyDecision,
    Segment,
)
from tiffany_plugin_repeater.storage import SCHEMA, Store


class LearningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="repeater-intelligence-")
        self.directory = Path(self.temp.name)
        self.config = RepeaterConfig(cooldown_seconds=0, answer_threshold=3, answer_weights=(1,),
                                     duplicate_window=1, proactive_interval_seconds=3600)
        self.store = Store(self.directory, self.config)
        await self.store.open()
        await self.store.ensure("g", "account", "group")
        self.seq = 0
        self.now = time.time()

    async def asyncTearDown(self):
        await self.store.close()
        self.temp.cleanup()

    def event(self, group="g", author="human", reply_to=None):
        self.seq += 1
        return SimpleNamespace(group_key=group, namespace="account", event_key=f"{group}:{self.seq}",
                               message_id=str(self.seq), author=author, reply_to=reply_to)

    async def say(self, value, *, now=None, author="human", event=None, quoted=None):
        content = Content.text(value) if isinstance(value, str) else value
        event = event or self.event(author=author)
        now = self.now + self.seq if now is None else now
        history, _ = await self.store.history(event.group_key, now)
        self.assertTrue(await self.store.observe(event, content, history, now, quoted=quoted))
        return event, content

    async def train(self, prompt, answer, count=3, prefix=None):
        for i in range(count):
            await self.store.barrier("g")
            self.now += 301
            if prefix:
                await self.say(prefix, now=self.now, author=f"a{i}")
            await self.say(prompt, now=self.now + 1, author=f"a{i}")
            await self.say(answer, now=self.now + 2, author=f"a{i}")

    async def episode(self, answer=None, *, now=None, complete=True, connection="test", previous=""):
        now = self.now if now is None else now
        answer = answer or Content.text("建议直接住公司")
        prompt = Content.text("又要加班")
        history = ({"content_key": previous, "event_key": "before", "created": now - 1},) if previous else ()
        decision = ReplyDecision(answer, prompt.key, prompt.dump(), "trigger", history, answer.key, source="graph")
        episode = await self.store.begin_episode("g", decision, connection, now)
        self.seq += 1
        mid = f"sent:{self.seq}"
        await self.store.add_receipt(episode, "g", DeliveryReceipt(mid, answer, now))
        await self.store.finish_episode(episode, complete)
        return episode, mid, decision

    async def test_observation_transaction_rolls_back_claim_and_legacy_learning(self):
        await self.say("前面这一句")
        event = self.event()
        history, _ = await self.store.history("g", self.now + self.seq)
        with patch.object(self.store, "_link", side_effect=RuntimeError("injected failure")):
            with self.assertRaises(RuntimeError):
                await self.store.observe(event, Content.text("后面这一句"), history, self.now + self.seq)
        self.assertFalse(await self.store.read(lambda db: db.execute("SELECT 1 FROM events WHERE key=?", (event.event_key,)).fetchone()))
        self.assertEqual(len(await self.store.recent("g")), 1)
        self.assertTrue(await self.store.observe(event, Content.text("后面这一句"), history, self.now + self.seq))
        self.assertFalse(await self.store.observe(event, Content.text("后面这一句"), history, self.now + self.seq))

    async def test_independence_and_rebuild_preserve_raw_facts(self):
        for _ in range(4):
            await self.store.barrier("g")
            await self.say("今天又加班", now=self.now)
            await self.say("建议直接住公司", now=self.now + 1)
        row = await self.store.read(lambda db: dict(db.execute("SELECT * FROM reply_edges WHERE prompt=?", (Content.text("今天又加班").key,)).fetchone()))
        self.assertEqual((row["raw_count"], row["independent_count"], row["effective_count"]), (4, 1, .5))
        before = await self.store.read(lambda db: [tuple(r) for r in db.execute("SELECT context,answer,raw_count,effective_count,independent_count FROM context_counts ORDER BY context,answer")])
        await self.store.rebuild_learning("g")
        after = await self.store.read(lambda db: [tuple(r) for r in db.execute("SELECT context,answer,raw_count,effective_count,independent_count FROM context_counts ORDER BY context,answer")])
        self.assertEqual(before, after)

    async def test_quote_targets_human_not_intervening_message(self):
        first, prompt = await self.say("明天几点上线")
        await self.say("今天外卖怎么还没到")
        quote = await self.store.quoted_human("g", first.message_id, self.now + 10)
        await self.say("九点左右吧", event=self.event(reply_to=first.message_id), quoted=quote)
        row = await self.store.read(lambda db: dict(db.execute("SELECT * FROM reply_edges WHERE answer=?", (Content.text("九点左右吧").key,)).fetchone()))
        self.assertEqual(row["prompt"], prompt.key)
        self.assertEqual(row["effective_count"], 1)
        self.assertFalse(await self.store.read(lambda db: db.execute("SELECT 1 FROM context_counts WHERE depth>1 AND answer=?", (Content.text("九点左右吧").key,)).fetchone()))

    async def test_timeout_and_reopen_cut_context(self):
        await self.say("第一段话", now=self.now)
        history, _ = await self.store.history("g", self.now + 121)
        self.assertFalse(history)
        await self.say("第二段话", now=self.now + 121)
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM reply_edges").fetchone()[0]), 0)
        await self.store.close()
        await self.store.open()
        self.assertFalse((await self.store.history("g", self.now + 122))[0])

    async def test_context_changes_distribution_for_identical_short_prompt(self):
        await self.train("不干了", "明天还得打卡", prefix="今天需求又做不完")
        await self.train("不干了", "下一把继续排位", prefix="排位又掉分了")
        prompt = Content.text("不干了")
        for prefix, preferred in (("今天需求又做不完", "明天还得打卡"), ("排位又掉分了", "下一把继续排位")):
            history = [{"content_key": Content.text(prefix).key}]
            bundle = await self.store.intelligence_bundle("g", prompt, history)
            distribution = contextual_distribution(bundle["rows"], bundle["query"], bundle["texts"], bundle["neighbors"])
            self.assertEqual(max(distribution, key=distribution.get), Content.text(preferred).key)

    def test_lexical_matching_keeps_negation_and_numbers(self):
        self.assertEqual(lexical_similarity("我明天去北京", "我明天不去北京"), 0)
        self.assertEqual(lexical_similarity("明天九点 123", "明天九点 124"), 0)
        self.assertEqual(lexical_similarity("明天星期三记得来打卡", "明天星期四记得来打卡"), 0)
        self.assertEqual(lexical_similarity("明天不想继续加班了", "明天不想不继续加班了"), 0)
        self.assertEqual(lexical_similarity("又要加班了！", "又要加班了"), 1)

    def test_third_order_needs_five_independent_supports_and_backs_off(self):
        from tiffany_plugin_repeater.context import packed
        query = ["a", "b", "c"]
        rows = [dict(depth=1, context=packed(["c"]), answer="short", independent_count=3, effective_count=1.5),
                dict(depth=2, context=packed(["b", "c"]), answer="medium", independent_count=3, effective_count=1.5),
                dict(depth=3, context=packed(query), answer="long", independent_count=4, effective_count=2)]
        distribution = contextual_distribution(rows, query, {}, {})
        self.assertNotIn("long", distribution)
        self.assertAlmostEqual(distribution["medium"], 1.5 / 6.5)
        rows[-1].update(independent_count=5, effective_count=2.5)
        distribution = contextual_distribution(rows, query, {}, {})
        self.assertAlmostEqual(distribution["long"], 2.5 / 7.5)
        self.assertAlmostEqual(sum(distribution.values()), 1)

    async def test_feedback_distinct_people_cancellation_late_addition_and_sarcasm(self):
        episode, mid, _ = await self.episode()
        await self.store.record_feedback("g", mid, "one", "reaction", "1", True, self.now + 1)
        await self.store.record_feedback("g", mid, "one", "reaction", "1", True, self.now + 2)
        await self.store.record_feedback("g", mid, "one", "reaction", "2", True, self.now + 2)
        await self.store.record_feedback("g", mid, "one", "quote", "mocking-message", True, self.now + 3)
        await self.store.record_feedback("g", mid, "two", "reaction", "1", True, self.now + 3)
        await self.store.record_feedback("g", mid, "", "reaction", "1", True, self.now + 3)
        row = await self.store.receipt_episode("g", mid)
        self.assertEqual((row["participants"], row["reward"]), (2, .8))
        await self.store.settle_feedback(self.now + 1801)
        await self.store.record_feedback("g", mid, "late", "quote", "late-reply", True, self.now + 1802)
        await self.store.record_feedback("g", mid, "two", "reaction", "1", False, self.now + 1803)
        row = await self.store.receipt_episode("g", mid)
        self.assertEqual((row["participants"], row["reward"]), (1, .5))
        stat = await self.store.read(lambda db: dict(db.execute("SELECT * FROM feedback_stats").fetchone()))
        self.assertEqual((stat["exposures"], stat["reward"]), (1, .5))
        # Removing a reaction does not remove that person's independent quote.
        await self.store.record_feedback("g", mid, "one", "reaction", "1", False, self.now + 1804)
        await self.store.record_feedback("g", mid, "one", "reaction", "2", False, self.now + 1804)
        self.assertEqual((await self.store.receipt_episode("g", mid))["participants"], 1)

    async def test_feedback_weights_and_experience_never_change_human_counts(self):
        before = await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM reply_edges").fetchone()[0])
        for i in range(3):
            _, mid, decision = await self.episode(now=self.now + i * 301)
            await self.store.record_feedback("g", mid, f"user{i}", "quote", f"reply{i}", True, self.now + i * 301 + 1)
        await self.store.settle_feedback(self.now + 3000)
        multipliers = await self.store.feedback_weights("g", decision.trigger_key, "")
        self.assertGreater(multipliers[decision.content.key], 1)
        extra = await self.store.intelligence_bundle("g", Content.load(decision.trigger_payload), [])
        self.assertEqual(len(extra["experience"]), 1)
        self.assertFalse(await self.store.feedback_weights("g", decision.trigger_key, "other-context"))
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM reply_edges").fetchone()[0]), before)

    async def test_partial_missing_receipt_disconnect_and_restart_are_not_exposures(self):
        _, mid, _ = await self.episode(complete=False)
        await self.store.record_feedback("g", mid, "one", "quote", "reply", True, self.now + 1)
        await self.episode(connection="broken")
        await self.store.invalidate_observations(connection_id="broken")
        await self.episode(connection="restart")
        await self.store.close()
        await self.store.open()
        await self.store.settle_feedback(self.now + 2000)
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM feedback_stats").fetchone()[0]), 0)

    async def test_out_of_order_feedback_matches_exact_receipt(self):
        collector = FeedbackCollector(self.store)
        observation = FeedbackObservation("g", "future", "one", "reaction", "128", True, self.now)
        self.assertFalse(await collector.capture(observation))
        decision = ReplyDecision(Content.text("测试回答"), trigger_key="trigger")
        episode = await self.store.begin_episode("g", decision, "test", self.now - 1)
        await self.store.add_receipt(episode, "g", DeliveryReceipt("future", decision.content, self.now))
        await collector.flush(self.now + 1)
        self.assertFalse(collector.pending)
        self.assertEqual((await self.store.receipt_episode("g", "future"))["participants"], 1)

    async def test_unattributed_feedback_is_bounded_expires_and_never_guesses(self):
        collector = FeedbackCollector(self.store)
        for i in range(1001):
            await collector.capture(FeedbackObservation("g", f"unknown:{i}", "one", "reaction", "1", True, self.now))
        self.assertEqual(len(collector.pending), 1000)
        self.assertEqual(self.store.metrics["feedback_buffer_overflow"], 1)
        await collector.flush(self.now + 61)
        self.assertFalse(collector.pending)
        self.assertEqual(self.store.metrics["feedback_buffer_expired"], 1000)
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM feedback_events").fetchone()[0]), 0)

    async def test_multi_image_receipts_count_one_person_once(self):
        episode, mid, decision = await self.episode()
        await self.store.add_receipt(episode, "g", DeliveryReceipt("second-image", decision.content, self.now))
        for target in (mid, "second-image"):
            await self.store.record_feedback("g", target, "same-user", "reaction", "1", True, self.now + 1)
        self.assertEqual((await self.store.receipt_episode("g", mid))["participants"], 1)

    async def test_feedback_changes_selection_probability_only_in_matching_context(self):
        prompt, first, second = "又要加班", "建议直接住公司", "建议老板自己干"
        await self.train(prompt, first)
        await self.train(prompt, second)
        for i in range(3):
            _, mid, decision = await self.episode(Content.text(first), now=self.now + i * 301)
            await self.store.record_feedback("g", mid, f"user{i}", "quote", f"reply{i}", True, self.now + i * 301 + 1)
        await self.store.settle_feedback(self.now + 3000)
        engine = Engine(self.store, self.config, random.Random(3))
        decision = await engine.decide(self.event(), Content.text(prompt))
        favored = next(r for r in decision.diagnostics if r["answer_key"] == Content.text(first).key)
        self.assertGreater(favored["feedback_multiplier"], 1)
        self.assertGreater(favored["probability"], favored["base_probability"])
        self.assertAlmostEqual(sum(r["probability"] for r in decision.diagnostics), 1)

    async def test_observe_preserves_legacy_rng_and_records_unsent_shadow(self):
        await self.train("又要加班", "建议老板自己干")
        await self.train("又要加班", "明天还得打卡")
        legacy = Engine(self.store, replace(self.config, intelligence_mode="off"), random.Random(4))
        observe = Engine(self.store, replace(self.config, intelligence_mode="observe"), random.Random(4))
        for _ in range(5):
            left = await legacy.decide(self.event(), Content.text("又要加班"))
            right = await observe.decide(self.event(), Content.text("又要加班"))
            self.assertEqual(left.content, right.content)
            self.assertEqual(legacy.rng.getstate(), observe.rng.getstate())
            self.assertIsNotNone(right.shadow)
        def remove_legacy(db):
            with db:
                db.execute("DELETE FROM relations")
                db.execute("DELETE FROM variants")
        await self.store.call(remove_legacy)
        decision = await observe.decide(self.event(), Content.text("又要加班"))
        self.assertEqual(decision.mode, "shadow")
        episode = await self.store.begin_episode("g", decision, "test", self.now)
        record = await self.store.read(lambda db: dict(db.execute("SELECT * FROM reply_episodes WHERE id=?", (episode,)).fetchone()))
        self.assertEqual((record["status"], record["settled"], record["observable"]), ("shadow", 1, 0))

    async def test_evidence_capacity_decrements_support_and_invalidates_neighbors(self):
        self.store.config = replace(self.config, max_evidence_per_group=3)
        for _ in range(3):
            await self.store.barrier("g")
            self.now += 301
            await self.say("加班怎么办", now=self.now)
            await self.say("老板自己干", now=self.now + 1)
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), 3)
        row = await self.store.read(lambda db: dict(db.execute("SELECT * FROM reply_edges WHERE prompt=?", (Content.text("加班怎么办").key,)).fetchone()))
        self.assertEqual((row["raw_count"], row["independent_count"]), (2, 2))
        self.assertGreater(await self.store.read(lambda db: db.execute("SELECT epoch FROM learning_state WHERE group_key='g'").fetchone()[0]), 0)

    async def test_legacy_capacity_counters_follow_rollback_and_group_clear(self):
        self.store.config = replace(self.config, max_messages_per_group=2)
        same = Content.text("两个群都保留的内容")
        await self.store.ensure("other", "account", "other")
        for i in range(4):
            await self.store.learn("g", same, "human", f"counter:{i}", self.now + i)
        await self.store.learn("other", same, "human", "counter:other", self.now)
        counts = await self.store.read(lambda db: (db.execute("SELECT SUM(count) FROM message_group_counts").fetchone()[0],
                                                   db.execute("SELECT count FROM message_content_counts WHERE content_key=?", (same.key,)).fetchone()[0]))
        self.assertEqual(counts, (3, 3))
        def abort(db):
            with db:
                db.execute("DELETE FROM messages WHERE group_key='g'")
                raise RuntimeError("rollback after count triggers")
        with self.assertRaises(RuntimeError):
            await self.store.call(abort)
        await self.store.clear("g")
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT count FROM message_content_counts WHERE content_key=?", (same.key,)).fetchone()[0]), 1)
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT SUM(count) FROM message_hour_counts").fetchone()[0]), 1)

    async def test_observe_retains_large_legacy_pool_and_original_random_sequence(self):
        variants = [Content.text(f"第{i}种独立回复") for i in range(205)]
        pool, weights = [(variants, [1] * len(variants))], [1]
        origins = {c.key: c.key for c in variants}
        engines = [Engine(self.store, replace(self.config, intelligence_mode=mode), random.Random(17)) for mode in ("off", "observe")]
        results = []
        for engine in engines:
            with patch.object(engine, "_legacy_buckets", new=AsyncMock(return_value=(pool, weights, origins, 3))):
                results.append(await engine.decide(self.event(), Content.text("触发大候选池")))
        self.assertEqual(results[0].content, results[1].content)
        self.assertEqual(engines[0].rng.getstate(), engines[1].rng.getstate())
        self.assertAlmostEqual(results[1].probability, 1 / 205)

    async def test_candidate_reads_stay_batched_and_auto_limits_distinct_answers(self):
        prompt = Content.text("今天又要加班")
        answers = [Content.text("独立回复，建议老板自己干" + format(i, "08b").translate(str.maketrans("01", "！？"))) for i in range(205)]
        self.assertEqual(len({answer.words for answer in answers}), 1)
        engine = Engine(self.store, self.config, random.Random(4))
        async def select():
            statements = []
            await self.store.read(lambda db: db.set_trace_callback(statements.append))
            try:
                decision = await engine.decide(self.event(), prompt)
            finally:
                await self.store.read(lambda db: db.set_trace_callback(None))
            reads = [sql for sql in statements if sql.lstrip().upper().startswith(("SELECT", "WITH"))]
            self.assertLessEqual(len(reads), 10)
            return decision, len(reads)
        for index, answer in enumerate(answers):
            await self.store.barrier("g")
            await self.store.learn("g", prompt, "human", f"pool:prompt:{index}", self.now)
            await self.store.learn("g", answer, "human", f"pool:answer:{index}", self.now + 1)
            if index == 3:
                _, small_reads = await select()
        decision, large_reads = await select()
        self.assertEqual(small_reads, large_reads)
        self.assertIn(decision.content.key, sorted(c.key for c in answers)[:200])
        self.assertAlmostEqual(decision.probability, 1 / 200)

    async def test_forget_processed_answer_also_removes_raw_source_and_experience(self):
        raw = Content.text("龟龟，建议老板自己干")
        actual = raw.answer(addressed=True)
        await self.train("加班怎么办", raw)
        decision = ReplyDecision(actual, trigger_key=Content.text("加班怎么办").key, raw_answer_key=raw.key)
        token, episode = await self.store.reserve("g", actual, self.now, self.config, decision=decision, connection_id="test")
        await self.store.add_receipt(episode, "g", DeliveryReceipt("forget-target", actual, self.now))
        await self.store.finish(token, self.now, True, episode_id=episode, complete=True)
        self.assertTrue(await self.store.forget("g"))
        self.assertTrue(await self.store.banned("g", raw))
        self.assertTrue(await self.store.banned("g", actual))
        self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM reply_edges").fetchone()[0]), 0)
        self.assertFalse(await self.store.receipt_episode("g", "forget-target"))

    async def test_forget_removes_snapshot_reference_even_when_quote_uses_another_target(self):
        target, _ = await self.say("明天几点上线")
        await self.store.barrier("g")
        _, forgotten = await self.say("这句话需要彻底遗忘")
        await self.say("随便一个插话")
        quote = await self.store.quoted_human("g", target.message_id, self.now + 10)
        reply, _ = await self.say("九点一起上线", event=self.event(reply_to=target.message_id), quoted=quote)
        token = await self.store.reserve("g", forgotten, self.now, self.config)
        await self.store.finish(token, self.now, True)
        await self.store.forget("g")
        self.assertFalse(await self.store.read(lambda db: db.execute("SELECT 1 FROM learning_evidence WHERE event_key=?", (reply.event_key,)).fetchone()))
        await self.store.collect_utterances()
        self.assertFalse(await self.store.read(lambda db: db.execute("SELECT 1 FROM utterances WHERE group_key='g' AND content_key=?", (forgotten.key,)).fetchone()))

    async def test_missing_message_id_is_not_an_exposure(self):
        decision = ReplyDecision(Content.text("缺少平台回执的回答"), trigger_key="q")
        episode = await self.store.begin_episode("g", decision, "test", self.now)
        await self.store.add_receipt(episode, "g", DeliveryReceipt(None, decision.content, self.now))
        await self.store.finish_episode(episode, True)
        await self.store.settle_feedback(self.now + 2000)
        self.assertFalse(await self.store.read(lambda db: db.execute("SELECT * FROM feedback_stats").fetchone()))

    async def test_pruning_subtracts_evidence_and_feedback_contributions(self):
        await self.train("今天又加班", "建议直接住公司")
        await self.episode(now=self.now - 31 * 86400)
        await self.store.settle_feedback(self.now)
        await self.store.cleanup(self.now + 91 * 86400)
        counts = await self.store.read(lambda db: [db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("learning_evidence", "context_counts", "reply_edges", "feedback_stats", "delivery_receipts", "utterances")])
        self.assertEqual(counts, [0] * len(counts))

    async def test_graph_borrows_only_reliable_local_replies_and_rejects_stale_commit(self):
        prompt_a, prompt_b = "今天又要加班了", "老板今天还不让走"
        for prompt in (prompt_a, prompt_b):
            for answer in ("建议老板自己干", "什么时候才能辞职"):
                await self.train(prompt, answer)
        await self.train(prompt_a, "明天还得继续打卡")
        face = Content((Segment("face", 128),))
        await self.train(prompt_a, face)
        for i in range(18):
            await self.train(f"独立话题{i}", f"独立回答{i}", count=1)
        worker = GraphWorker(self.store)
        try:
            for _ in range(3):
                await worker.tick()
            neighbors = await self.store.read(lambda db: [dict(r) for r in db.execute("SELECT * FROM expression_neighbors WHERE source=?", (Content.text(prompt_b).key,))])
            self.assertIn(Content.text(prompt_a).key, [r["target"] for r in neighbors])
            bundle = await self.store.intelligence_bundle("g", Content.text(prompt_b), [])
            self.assertIn(Content.text(prompt_a).key, bundle["graph_sources"])
            self.assertIn(face.key, bundle["utterances"])
            decision = await Engine(self.store, self.config, random.Random(5)).decide(self.event(), Content.text(prompt_b))
            face_candidate = next(row for row in decision.diagnostics if row["answer_key"] == face.key)
            self.assertIn("graph", face_candidate["sources"])
            await self.store.ensure("elsewhere", "account", "another-group")
            self.assertFalse((await self.store.intelligence_bundle("elsewhere", Content.text(prompt_b), []))["graph_sources"])
            # Snapshot then clear: a worker may finish computation after the deletion.
            def dirty(db):
                with db:
                    db.execute("INSERT OR REPLACE INTO graph_dirty VALUES('g',?,1)", (Content.text(prompt_b).key,))
            await self.store.call(dirty)
            snapshot = await self.store.graph_snapshot()
            result = build_neighbors(snapshot)
            await self.store.clear("g")
            self.assertFalse(await self.store.commit_neighbors(snapshot, result, self.now))
            self.assertEqual(await self.store.read(lambda db: db.execute("SELECT COUNT(*) FROM expression_neighbors").fetchone()[0]), 0)
        finally:
            await worker.close()

    def test_generic_shared_replies_do_not_connect_unrelated_topics(self):
        edges = []
        for i in range(20):
            for answer in ("哈哈", "哈哈哈", "哈哈！", "哈 哈...", "好呀", "嗯嗯", f"回答{i}"):
                edges.append({"prompt": f"topic{i}", "answer": answer, "answer_text": answer, "answer_media": 0, "independent_count": 4, "effective_count": 2})
        result = build_neighbors({"edges": edges, "dirty": [{"node": f"topic{i}"} for i in range(20)]}, budget=5)
        self.assertTrue(all(not neighbors for neighbors in result.values()))


class MigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_written_by_released_v021_preserves_corpus_and_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            shutil.copyfile(Path(__file__).with_name("fixtures") / "repeater.sqlite3", Path(directory) / "repeater.sqlite3")
            store = Store(directory, RepeaterConfig())
            await store.open()
            try:
                self.assertEqual((await store.settings("fixture-group")).preset, "活跃的")
                self.assertEqual(len(await store.recent("fixture-group")), 6)
                self.assertEqual(len(await store.recent("fixture-group", sent=True)), 1)
                self.assertTrue(await store.candidates("fixture-account", Content.text("今天还得加班").words))
                self.assertEqual(await store.read(lambda db: db.execute("SELECT SUM(count) FROM message_group_counts").fetchone()[0]), 6)
                self.assertEqual(await store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), 0)
            finally:
                await store.close()
            backups = list(Path(directory).glob("repeater.v1-backup-*.sqlite3"))
            self.assertEqual(len(backups), 1)
            db = sqlite3.connect(backups[0])
            try:
                self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)
            finally:
                db.close()

    async def test_failed_migration_rolls_back_all_schema_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "repeater.sqlite3"
            db = sqlite3.connect(path)
            db.executescript(SCHEMA)
            db.close()
            store = Store(directory, RepeaterConfig())
            with patch("tiffany_plugin_repeater.storage.SCHEMA_V2", "ALTER TABLE groups ADD COLUMN intelligence_mode TEXT; SELECT nonexistent_column;"):
                with self.assertRaises(sqlite3.OperationalError):
                    await store.open()
            db = sqlite3.connect(path)
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertNotIn("intelligence_mode", [r[1] for r in db.execute("PRAGMA table_info(groups)")])
            db.close()
            self.assertIsNone(store._executor)

    async def test_v1_backup_atomic_migration_and_no_fabricated_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "repeater.sqlite3"
            db = sqlite3.connect(path)
            db.executescript(SCHEMA)
            db.execute("INSERT INTO groups(key,namespace,target,enabled) VALUES('g','account','1',1)")
            db.commit()
            db.close()
            store = Store(directory, RepeaterConfig())
            await store.open()
            try:
                self.assertEqual(await store.read(lambda db: db.execute("PRAGMA user_version").fetchone()[0]), 2)
                self.assertEqual(await store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), 0)
            finally:
                await store.close()
            backups = list(Path(directory).glob("repeater.v1-backup-*.sqlite3"))
            self.assertEqual(len(backups), 1)
            db = sqlite3.connect(backups[0])
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM groups").fetchone()[0], 1)
            db.close()
            await store.open()
            await store.close()
            self.assertEqual(len(list(Path(directory).glob("repeater.v1-backup-*.sqlite3"))), 1)


class IntelligenceHostTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = native.NativeTests.asyncSetUp
    asyncTearDown = native.NativeTests.asyncTearDown
    message = native.NativeTests.message
    settle = native.NativeTests.settle
    current_event = native.NativeTests.current_event
    qqmessage = native.NativeTests.qqmessage

    async def test_receipt_storage_failure_does_not_resend_platform_message(self):
        from tiffany_plugin_repeater.messages import Incoming
        ctx = await self.message("发送记账故障之前的前文")
        event = Incoming.read(ctx)
        answer = Content.text("这条回答只能发一次")
        decision = ReplyDecision(answer, trigger_key="q")
        before = len(self.client.calls)
        with patch.object(self.service.store, "add_receipt", new=AsyncMock(side_effect=RuntimeError("receipt write failed"))):
            with self.assertLogs("tiffany_plugin_repeater.service", level="ERROR"):
                self.assertTrue(await self.service._send(event.group_key, decision, ctx))
        self.assertEqual(len(self.client.calls), before + 1)
        record = await self.service.store.read(lambda db: dict(db.execute("SELECT * FROM reply_episodes LIMIT 1").fetchone()))
        self.assertFalse(record["observable"])
        self.assertEqual(record["status"], "failed")

    async def test_reaction_and_mocking_quote_are_feedback_not_human_learning(self):
        for _ in range(3):
            await self.message("这是一个会让人接梗的测试")
        await self.settle()
        episode = await self.service.store.read(lambda db: dict(db.execute("SELECT * FROM reply_episodes LIMIT 1").fetchone()))
        receipt = await self.service.store.read(lambda db: dict(db.execute("SELECT * FROM delivery_receipts LIMIT 1").fetchone()))
        before = await self.service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0])
        raw = {"post_type": "notice", "notice_type": "group_msg_emoji_like", "self_id": 9,
               "group_id": 1, "user_id": 3, "message_id": receipt["message_id"], "likes": [{"emoji_id": "1", "count": 20}], "is_add": True}
        await self.bot.emit(Envelope("napcat", raw, client=self.client, kind="notice", adapter_id="onebot:test"))
        await self.message(segments=[{"type": "reply", "data": {"id": receipt["message_id"]}}, {"type": "text", "data": {"text": "你可真是个人才，闭嘴吧你"}}], author=4)
        row = await self.service.store.receipt_episode(receipt["group_key"], receipt["message_id"])
        self.assertEqual((row["participants"], row["reward"]), (2, .8))
        self.assertEqual(await self.service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), before)
        self.assertFalse((await self.service.store.history(receipt["group_key"], time.time()))[0])

    async def test_intelligence_command_permissions_and_group_override(self):
        await self.message("/repeater intelligence off", author=2)
        event = await self.current_event()
        self.assertIsNone((await self.service.store.settings(event.group_key)).intelligence_mode)
        await self.message("/repeater intelligence off", author=7)
        self.assertEqual((await self.service.store.settings(event.group_key)).intelligence_mode, "off")
        before = await self.service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0])
        await self.message("停用智能以后依旧可以学习旧语料")
        self.assertEqual(await self.service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), before)
        await self.message("/repeater intelligence auto", author=7)
        await self.message("重新开启以后收集新证据")
        self.assertEqual(await self.service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), before + 1)

    async def test_reconnection_invalidates_windows_for_entire_account(self):
        for group in (1, 2):
            for _ in range(3):
                await self.message("同一账户连接上的回答", group=group)
            await self.settle()
        await self.message("重连以后的新消息", client=FakeOneBot())
        self.assertEqual(await self.service.store.read(lambda db: db.execute("SELECT COUNT(*) FROM reply_episodes WHERE observable=1").fetchone()[0]), 0)

    async def test_qq_quote_feedback_uses_original_opaque_message_id(self):
        for _ in range(3):
            await self.qqmessage("QQ 官方引用反馈测试")
        await self.settle()
        receipt = await self.service.store.read(lambda db: dict(db.execute("SELECT * FROM delivery_receipts LIMIT 1").fetchone()))
        raw = {"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": "quote-openid", "group_openid": "qq-group",
               "author": {"member_openid": "another-openid"}, "content": "你可真是个人才",
               "message_reference": {"message_id": receipt["message_id"]}}}
        await self.bot.emit(Envelope("qq", raw, client=self.qq, kind="message", adapter_id="qq:test-app", session_id="qq-group"))
        self.assertEqual((await self.service.store.receipt_episode(receipt["group_key"], receipt["message_id"]))["participants"], 1)

    async def test_new_dashboard_views_are_authenticated_paginated_and_read_only(self):
        from tiffany_plugin_repeater.dashboard import Dashboard
        for _ in range(3):
            await self.message("观察台新的回答依据")
        await self.settle()
        self.service.config = replace(self.config, dashboard_enabled=True, dashboard_token="test-intelligence-token", dashboard_port=0)
        dashboard = Dashboard(self.service)
        await dashboard.open()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                base = f"http://127.0.0.1:{dashboard.port}"
                for view in ("evidence", "neighbors", "decisions", "feedback"):
                    async with session.get(base + "/api/" + view) as response:
                        self.assertEqual(response.status, 401)
                    async with session.get(base + "/api/" + view, headers={"Authorization": "Bearer test-intelligence-token"}, params={"limit": "1"}) as response:
                        self.assertEqual(response.status, 200)
                        self.assertLessEqual(len((await response.json())["rows"]), 1)
                    async with session.post(base + "/api/" + view, headers={"Authorization": "Bearer test-intelligence-token"}) as response:
                        self.assertEqual(response.status, 405)
                episode = await self.service.store.read(lambda db: db.execute("SELECT id FROM reply_episodes LIMIT 1").fetchone()[0])
                detail = await dashboard.query("detail", {"table": "reply_episodes", "id": episode})
                self.assertTrue(detail["receipts"])
                self.assertIn("probability", detail["record"]["decision"])
                self.assertEqual((await dashboard.query("overview", {}))["learning"]["learning_evidence"], 3)
        finally:
            await dashboard.close()
