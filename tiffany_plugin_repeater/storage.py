"""Native schema, commit-before-send bookkeeping and one owned SQLite thread."""
import asyncio
import sqlite3
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .context import packed
from .feedback_store import FeedbackStore
from .learning_store import NEW_TABLES, SCHEMA_V2, LearningStore
from .models import Content, GroupSettings
from .retrieval_store import RetrievalStore

SCHEMA = """
CREATE TABLE groups (
 key TEXT PRIMARY KEY, namespace TEXT NOT NULL, target TEXT NOT NULL,
 enabled INTEGER NOT NULL, text_enabled INTEGER NOT NULL DEFAULT 1,
 expression_enabled INTEGER NOT NULL DEFAULT 1, media_enabled INTEGER NOT NULL DEFAULT 0,
 preset TEXT NOT NULL DEFAULT '沉默的', barrier INTEGER NOT NULL DEFAULT 0);
CREATE TABLE messages (
 id INTEGER PRIMARY KEY, group_key TEXT NOT NULL, author TEXT NOT NULL, event_key TEXT,
 created REAL NOT NULL, content_key TEXT NOT NULL, payload TEXT NOT NULL, words TEXT NOT NULL,
 media INTEGER NOT NULL);
CREATE INDEX messages_group ON messages(group_key,id DESC);
CREATE TABLE relations (
 group_key TEXT NOT NULL, prompt TEXT NOT NULL, signature TEXT NOT NULL,
 count INTEGER NOT NULL, updated REAL NOT NULL, PRIMARY KEY(group_key,prompt,signature));
CREATE INDEX relations_prompt ON relations(prompt);
CREATE TABLE variants (
 group_key TEXT NOT NULL, prompt TEXT NOT NULL, signature TEXT NOT NULL,
 content_key TEXT NOT NULL, payload TEXT NOT NULL, words TEXT NOT NULL,
 media INTEGER NOT NULL, count INTEGER NOT NULL, updated REAL NOT NULL,
 PRIMARY KEY(group_key,prompt,signature,content_key));
CREATE TABLE sent (
 id INTEGER PRIMARY KEY, group_key TEXT NOT NULL, created REAL NOT NULL,
 content_key TEXT NOT NULL, payload TEXT NOT NULL, words TEXT NOT NULL);
CREATE INDEX sent_group ON sent(group_key,id DESC);
CREATE TABLE bans (group_key TEXT NOT NULL, kind TEXT NOT NULL, value TEXT NOT NULL,
 PRIMARY KEY(group_key,kind,value));
CREATE TABLE events (key TEXT PRIMARY KEY, created REAL NOT NULL);
CREATE TABLE reservations (id TEXT PRIMARY KEY, group_key TEXT UNIQUE NOT NULL,
 created REAL NOT NULL, payload TEXT NOT NULL);
PRAGMA user_version=1;
"""

COLUMNS = {
    "groups": ("key", "namespace", "target", "enabled", "text_enabled", "expression_enabled", "media_enabled", "preset", "barrier"),
    "messages": ("id", "group_key", "author", "event_key", "created", "content_key", "payload", "words", "media"),
    "relations": ("group_key", "prompt", "signature", "count", "updated"),
    "variants": ("group_key", "prompt", "signature", "content_key", "payload", "words", "media", "count", "updated"),
    "sent": ("id", "group_key", "created", "content_key", "payload", "words"),
    "bans": ("group_key", "kind", "value"), "events": ("key", "created"),
    "reservations": ("id", "group_key", "created", "payload"),
}


class StorageBusy(RuntimeError):
    pass


class QueryTimeout(RuntimeError):
    pass


class Store(LearningStore, FeedbackStore, RetrievalStore):
    def __init__(self, directory, config):
        self.path = Path(directory) / "repeater.sqlite3"
        self.config = config
        self._executor = None
        self._db = None
        self._pending = set()
        self._accepting = False
        self._reader = None
        self._read_executor = None
        self._inspection = None
        self._inspection_executor = None
        self.metrics = Counter()
        self._gc_epochs = {}

    async def open(self):
        self._gc_epochs.clear()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tiffany-repeater")
        try:
            await self._submit(self._open)
            self._read_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="repeater-read")
            await self._submit(self._open_reader, reader=True)
            self._inspection_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="repeater-dashboard")
            await self._submit(self._open_inspection, reader="inspect")
        except BaseException:
            await self.close()
            raise
        self._accepting = True

    def _open(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.create_function("content_text", 1, lambda value: Content.load(value).plain, deterministic=True)
        db.create_function("content_media", 1, lambda value: Content.load(value).media, deterministic=True)
        try:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if version not in (0, 1, 2) or version == 0 and tables:
                raise ValueError("unsupported repeater database; original file retained")
            if version in (1, 2):
                for table, expected in COLUMNS.items():
                    if version == 2 and table == "groups":
                        expected = (*expected, "intelligence_mode")
                    actual = tuple(r[1] for r in db.execute(f'PRAGMA table_info("{table}")'))
                    if actual != expected:
                        raise ValueError(f"invalid repeater schema: {table}; original file retained")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            # The default 2 MiB cache continually evicts the active index pages
            # when hundreds of groups write into a large evidence corpus.
            db.execute("PRAGMA cache_size=-65536")
            db.execute("PRAGMA mmap_size=268435456")
            if not version:
                db.executescript("BEGIN IMMEDIATE;" + SCHEMA + SCHEMA_V2 + "COMMIT;")
            elif version == 1:
                backup = self.path.with_name("repeater.v1-backup-" + uuid.uuid4().hex[:12] + ".sqlite3")
                destination = sqlite3.connect(backup)
                try:
                    db.backup(destination)
                finally:
                    destination.close()
                try:
                    db.executescript("BEGIN IMMEDIATE;" + SCHEMA_V2 + "COMMIT;")
                except BaseException:
                    db.rollback()
                    raise
            if version == 2:
                expected_db = sqlite3.connect(":memory:")
                try:
                    expected_db.executescript(SCHEMA + SCHEMA_V2)
                    for table in NEW_TABLES:
                        expected = tuple(tuple(r)[1:6] for r in expected_db.execute(f'PRAGMA table_info("{table}")'))
                        actual = tuple(tuple(r)[1:6] for r in db.execute(f'PRAGMA table_info("{table}")'))
                        if actual != expected:
                            raise ValueError(f"invalid repeater schema: {table}; original file retained")
                    for trigger in ("messages_insert_stats", "messages_delete_stats"):
                        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?", (trigger,)).fetchone():
                            raise ValueError(f"invalid repeater schema: {trigger}; original file retained")
                finally:
                    expected_db.close()
            with db:
                db.execute("DELETE FROM reservations")
                # Repair totals left behind by earlier forget operations.
                self._refresh_relations(db)
                db.execute("UPDATE learning_state SET segment=segment+1,tail='[]'")
                db.execute("UPDATE groups SET barrier=1")
                db.execute("UPDATE reply_episodes SET observable=0,status=CASE WHEN status='pending' THEN 'incomplete' ELSE status END WHERE settled=0")
        except BaseException:
            db.close()
            raise
        self._db = db

    def _settle(self, future):
        self._pending.discard(future)
        if not future.cancelled():
            future.exception()

    def _open_reader(self):
        self._reader = self._connect_reader()

    def _open_inspection(self):
        self._inspection = self._connect_reader()

    def _connect_reader(self):
        connection = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA cache_size=-32768")
        connection.execute("PRAGMA mmap_size=268435456")
        connection.create_function("content_text", 1, lambda value: Content.load(value).plain, deterministic=True)
        connection.create_function("content_media", 1, lambda value: Content.load(value).media, deterministic=True)
        return connection

    async def _submit(self, fn, *, reader=False):
        executor = self._inspection_executor if reader == "inspect" else self._read_executor if reader else self._executor
        future = asyncio.wrap_future(executor.submit(fn))
        self._pending.add(future)
        future.add_done_callback(self._settle)
        try:
            return await asyncio.shield(future)
        finally:
            # An already-completed await need not yield to done callbacks.
            # Capacity must count outstanding work, even in a tight async loop.
            if future.done():
                self._pending.discard(future)

    async def call(self, function, *, critical=False):
        if not self._accepting or not critical and len(self._pending) >= self.config.max_storage_operations:
            self.metrics["storage_busy"] += 1
            raise StorageBusy("repeater store stopped or at capacity")
        return await self._submit(lambda: function(self._db))

    async def read(self, function):
        if not self._accepting or len(self._pending) >= self.config.max_storage_operations:
            self.metrics["storage_busy"] += 1
            raise StorageBusy("repeater store stopped or at capacity")
        def run():
            self.metrics["read_calls"] += 1
            try:
                self._reader.execute("BEGIN")
                return function(self._reader)
            finally:
                self._reader.rollback()
        return await self._submit(run, reader=True)

    async def inspect(self, function, timeout=1.):
        if not self._accepting or len(self._pending) >= self.config.max_storage_operations:
            self.metrics["storage_busy"] += 1
            raise StorageBusy("repeater store stopped or at capacity")
        def run():
            deadline = time.monotonic() + timeout
            self._inspection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            try:
                self._inspection.execute("BEGIN")
                return function(self._inspection)
            except sqlite3.OperationalError as exc:
                if "interrupted" in str(exc):
                    self.metrics["dashboard_timeouts"] += 1
                    raise QueryTimeout("查询超时，请增加群或时间筛选") from exc
                raise
            finally:
                self._inspection.set_progress_handler(None, 0)
                self._inspection.rollback()
        return await self._submit(run, reader="inspect")

    async def close(self):
        self._accepting = False
        if self._executor is None:
            return
        if self._pending:
            await asyncio.shield(asyncio.gather(*tuple(self._pending), return_exceptions=True))
        if self._db is not None:
            await self._submit(self._db.close)
            self._db = None
        if self._reader is not None:
            await self._submit(self._reader.close, reader=True)
            self._reader = None
        if self._inspection is not None:
            await self._submit(self._inspection.close, reader="inspect")
            self._inspection = None
        if self._inspection_executor is not None:
            self._inspection_executor.shutdown(wait=False)
            self._inspection_executor = None
        if self._read_executor is not None:
            self._read_executor.shutdown(wait=False)
            self._read_executor = None
        self._executor.shutdown(wait=False)
        self._executor = None

    async def ensure(self, key, namespace, target):
        def run(db):
            with db:
                db.execute("INSERT OR IGNORE INTO groups(key,namespace,target,enabled) VALUES(?,?,?,?)",
                           (key, namespace, str(target), self.config.enabled_default))
        await self.call(run)

    async def settings(self, key):
        def run(db):
            row = db.execute("SELECT * FROM groups WHERE key=?", (key,)).fetchone()
            return GroupSettings(bool(row["enabled"]), bool(row["text_enabled"]), bool(row["expression_enabled"]), bool(row["media_enabled"]), row["preset"], row["intelligence_mode"]) if row else GroupSettings(enabled=self.config.enabled_default)
        return await self.call(run)

    async def configure(self, key, option, value):
        if option not in ("enabled", "text_enabled", "expression_enabled", "media_enabled", "preset", "intelligence_mode"):
            raise ValueError("unknown group option")
        if option == "intelligence_mode" and value not in ("auto", "observe", "off"):
            raise ValueError("invalid intelligence mode")
        def run(db):
            with db:
                db.execute(f"UPDATE groups SET {option}=?,barrier=1 WHERE key=?", (value, key))
                db.execute("UPDATE learning_state SET tail='[]',segment=segment+1 WHERE group_key=?", (key,))
                if option == "intelligence_mode" and value == "off" or option == "enabled" and not value:
                    db.execute("UPDATE reply_episodes SET observable=0 WHERE group_key=? AND settled=0", (key,))
        await self.call(run)

    async def barrier(self, key):
        def run(db):
            with db:
                db.execute("UPDATE groups SET barrier=1 WHERE key=?", (key,))
                db.execute("UPDATE learning_state SET tail='[]',segment=segment+1 WHERE group_key=?", (key,))
        await self.call(run)

    async def claim(self, event_key, now):
        def run(db):
            with db:
                return db.execute("INSERT OR IGNORE INTO events VALUES(?,?)", (event_key, now)).rowcount == 1
        return await self.call(run)

    async def learn(self, key, content, author, event_key, now):
        def run(db):
            with db:
                self._learn(db, key, content, author, event_key, now)
        await self.call(run)

    def _learn(self, db, key, content, author, event_key, now):
        group = db.execute("SELECT barrier FROM groups WHERE key=?", (key,)).fetchone()
        previous = db.execute("SELECT words FROM messages WHERE group_key=? ORDER BY id DESC LIMIT 1", (key,)).fetchone()
        if previous and not group[0]:
            signature = content.key if content.media else content.words
            params = (key, previous[0], signature)
            db.execute("INSERT INTO relations VALUES(?,?,?,1,?) ON CONFLICT(group_key,prompt,signature) DO UPDATE SET count=count+1,updated=excluded.updated", (*params, now))
            db.execute("INSERT INTO variants VALUES(?,?,?,?,?,?,?,1,?) ON CONFLICT(group_key,prompt,signature,content_key) DO UPDATE SET count=count+1,updated=excluded.updated",
                       (*params, content.key, content.dump(), content.words, content.media, now))
        db.execute("INSERT INTO messages(group_key,author,event_key,created,content_key,payload,words,media) VALUES(?,?,?,?,?,?,?,?)", (key, author, event_key, now, content.key, content.dump(), content.words, content.media))
        db.execute("UPDATE groups SET barrier=0 WHERE key=?", (key,))
        size = db.execute("SELECT count FROM message_group_counts WHERE group_key=?", (key,)).fetchone()[0]
        if size > self.config.max_messages_per_group:
            db.execute("DELETE FROM messages WHERE id IN (SELECT id FROM messages WHERE group_key=? ORDER BY id LIMIT ?)", (key, size - self.config.max_messages_per_group))

    async def recent(self, key, limit=100, *, sent=False):
        table = "sent" if sent else "messages"
        return await self.call(lambda db: [dict(r) for r in db.execute(f"SELECT * FROM {table} WHERE group_key=? ORDER BY id DESC LIMIT ?", (key, limit))])

    async def candidates(self, namespace, prompt):
        return await self.read(lambda db: [dict(r) for r in db.execute("""SELECT v.*,r.count AS relation_count
          FROM variants v JOIN relations r USING(group_key,prompt,signature)
          JOIN groups g ON g.key=v.group_key WHERE g.namespace=? AND v.prompt=?""", (namespace, prompt))])

    async def banned(self, key, content):
        return await self.call(lambda db: db.execute("SELECT 1 FROM bans WHERE group_key=? AND ((kind='content' AND value=?) OR (kind='keyword' AND value=?))", (key, content.key, content.words)).fetchone() is not None)

    async def reserve(self, key, content, now, config, force=False, *, decision=None, connection_id=""):
        def run(db):
            with db:
                db.execute("DELETE FROM reservations WHERE created<?", (now - 120,))
                if db.execute("SELECT 1 FROM reservations WHERE group_key=?", (key,)).fetchone():
                    return None
                if not force:
                    latest = db.execute("SELECT created FROM sent WHERE group_key=? ORDER BY id DESC LIMIT 1", (key,)).fetchone()
                    count = db.execute("SELECT COUNT(*) FROM sent WHERE group_key=? AND created>?", (key, now - config.burst_window_seconds)).fetchone()[0]
                    if (latest and now - latest[0] < config.cooldown_seconds) or count >= config.burst_limit:
                        return None
                token = uuid.uuid4().hex
                db.execute("INSERT INTO reservations VALUES(?,?,?,?)", (token, key, now, content.dump()))
                if decision is not None:
                    return token, self._begin_episode(db, key, decision, connection_id, now)
                return token
        return await self.call(run)

    async def finish(self, token, now, success, *, episode_id=None, complete=False):
        def run(db):
            with db:
                row = db.execute("SELECT * FROM reservations WHERE id=?", (token,)).fetchone()
                if row and success:
                    content = Content.load(row["payload"])
                    db.execute("INSERT INTO sent(group_key,created,content_key,payload,words) VALUES(?,?,?,?,?)", (row["group_key"], now, content.key, content.dump(), content.words))
                    db.execute("DELETE FROM sent WHERE group_key=? AND id NOT IN (SELECT id FROM sent WHERE group_key=? ORDER BY id DESC LIMIT ?)", (row["group_key"], row["group_key"], max(100, self.config.duplicate_window)))
                db.execute("DELETE FROM reservations WHERE id=?", (token,))
                if row and episode_id:
                    self._finish_episode(db, episode_id, complete)
        await self.call(run, critical=True)

    @staticmethod
    def _refresh_relations(db, key=None):
        scope = "group_key=? AND " if key is not None else ""
        args = (key,) if key is not None else ()
        match = "v.group_key=relations.group_key AND v.prompt=relations.prompt AND v.signature=relations.signature"
        db.execute(f"DELETE FROM relations WHERE {scope}NOT EXISTS (SELECT 1 FROM variants v WHERE {match})", args)
        totals = f"(SELECT SUM(v.count),MAX(v.updated) FROM variants v WHERE {match})"
        db.execute(f"""UPDATE relations SET (count,updated)={totals}
            WHERE {scope}(count,updated) IS NOT {totals}""", args)

    async def forget(self, key, keyword=False):
        def run(db):
            with db:
                row = db.execute("SELECT * FROM sent WHERE group_key=? ORDER BY id DESC LIMIT 1", (key,)).fetchone()
                if not row:
                    return False
                db.execute("INSERT OR IGNORE INTO bans VALUES(?,?,?)", (key, "keyword" if keyword else "content", row["words"] if keyword else row["content_key"]))
                if not keyword:
                    aliases = {row["content_key"]}
                    episode = db.execute("SELECT raw_answer_key FROM reply_episodes WHERE group_key=? AND answer_key=? AND status IN ('sent','partial') ORDER BY sent_at DESC LIMIT 1", (key, row["content_key"])).fetchone()
                    if episode:
                        aliases.add(episode[0])
                    encoded = packed(sorted(aliases))
                    db.executemany("INSERT OR IGNORE INTO bans VALUES(?,'content',?)", ((key, value) for value in aliases))
                    ids = [r[0] for r in db.execute("""SELECT DISTINCT e.id FROM learning_evidence e LEFT JOIN evidence_links l ON l.evidence_id=e.id
                      WHERE e.group_key=? AND (e.content_key IN (SELECT value FROM json_each(?))
                      OR l.answer IN (SELECT value FROM json_each(?)) OR EXISTS(SELECT 1 FROM json_each(l.context) WHERE value IN (SELECT value FROM json_each(?)))
                      OR EXISTS(SELECT 1 FROM json_each(e.history) WHERE json_extract(value,'$.content_key') IN (SELECT value FROM json_each(?))))""", (key, encoded, encoded, encoded, encoded))]
                    self._remove_evidence(db, ids)
                    self._remove_episodes(db, [r[0] for r in db.execute("""SELECT id FROM reply_episodes WHERE group_key=? AND
                      (answer_key IN (SELECT value FROM json_each(?)) OR raw_answer_key IN (SELECT value FROM json_each(?))
                      OR trigger_key IN (SELECT value FROM json_each(?)) OR EXISTS(SELECT 1 FROM json_each(history) WHERE json_extract(value,'$.content_key') IN (SELECT value FROM json_each(?))))""", (key, encoded, encoded, encoded, encoded))])
                    for table in ("messages", "variants", "sent"):
                        db.execute(f"DELETE FROM {table} WHERE group_key=? AND content_key IN (SELECT value FROM json_each(?))", (key, encoded))
                    self._refresh_relations(db, key)
                self._invalidate(db, key)
                db.execute("UPDATE groups SET barrier=1 WHERE key=?", (key,))
                return True
        return await self.call(run)

    async def clear(self, key):
        def run(db):
            with db:
                for table in ("messages", "relations", "variants", "sent", "bans", "reservations"):
                    db.execute(f"DELETE FROM {table} WHERE group_key=?", (key,))
                db.execute("UPDATE groups SET barrier=1 WHERE key=?", (key,))
                self._remove_episodes(db, [r[0] for r in db.execute("SELECT id FROM reply_episodes WHERE group_key=?", (key,))])
                for table in NEW_TABLES:
                    if table not in ("learning_state", "delivery_receipts", "reply_episodes", "feedback_events", "message_content_counts", "message_hour_counts"):
                        db.execute(f"DELETE FROM {table} WHERE group_key=?", (key,))
                self._invalidate(db, key)
        await self.call(run)

    async def cleanup(self, now):
        def run(db):
            with db:
                db.execute("DELETE FROM events WHERE created<?", (now - 86400,))
                db.execute("DELETE FROM relations WHERE updated<? AND count<=2", (now - self.config.stale_relation_days * 86400,))
                db.execute("DELETE FROM variants WHERE NOT EXISTS (SELECT 1 FROM relations r WHERE r.group_key=variants.group_key AND r.prompt=variants.prompt AND r.signature=variants.signature)")
                self._prune_learning(db, now)
        await self.call(run)
        await self.collect_utterances()

    async def cleanup_learning(self, now):
        def run(db):
            with db:
                self._prune_learning(db, now)
        await self.call(run)
        await self.collect_utterances()

    async def media_references(self):
        def run(db):
            found = set()
            for table in ("messages", "variants", "sent", "reservations", "utterances", "reply_episodes", "delivery_receipts"):
                for row in db.execute(f"SELECT DISTINCT payload FROM {table}"):
                    found.update(str(s.value) for s in Content.load(row[0]).segments if s.kind == "image")
            return found
        return await self.inspect(run, timeout=30.)
