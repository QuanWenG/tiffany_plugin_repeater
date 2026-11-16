"""Version-two evidence and derived statistics, owned by Store's SQLite thread."""
import json

from .context import (
    INDEPENDENCE_SECONDS,
    POLICY,
    POLICY_VERSION,
    SEGMENT_SECONDS,
    grams,
    normalize,
    packed,
)

MESSAGE_STATS_SCHEMA = """
CREATE TABLE message_group_counts(group_key TEXT PRIMARY KEY, count INTEGER NOT NULL);
CREATE TABLE message_content_counts(content_key TEXT PRIMARY KEY, count INTEGER NOT NULL);
CREATE TABLE message_hour_counts(hour INTEGER PRIMARY KEY, count INTEGER NOT NULL);
INSERT INTO message_group_counts SELECT group_key,COUNT(*) FROM messages GROUP BY group_key;
INSERT INTO message_content_counts SELECT content_key,COUNT(*) FROM messages GROUP BY content_key;
INSERT INTO message_hour_counts SELECT CAST(created/3600 AS INTEGER)*3600,COUNT(*) FROM messages GROUP BY CAST(created/3600 AS INTEGER);
CREATE TRIGGER messages_insert_stats AFTER INSERT ON messages BEGIN
 INSERT INTO message_group_counts VALUES(NEW.group_key,1) ON CONFLICT(group_key) DO UPDATE SET count=count+1;
 INSERT INTO message_content_counts VALUES(NEW.content_key,1) ON CONFLICT(content_key) DO UPDATE SET count=count+1;
 INSERT INTO message_hour_counts VALUES(CAST(NEW.created/3600 AS INTEGER)*3600,1) ON CONFLICT(hour) DO UPDATE SET count=count+1;
END;
CREATE TRIGGER messages_delete_stats AFTER DELETE ON messages BEGIN
 UPDATE message_group_counts SET count=count-1 WHERE group_key=OLD.group_key;
 DELETE FROM message_group_counts WHERE group_key=OLD.group_key AND count<=0;
 UPDATE message_content_counts SET count=count-1 WHERE content_key=OLD.content_key;
 DELETE FROM message_content_counts WHERE content_key=OLD.content_key AND count<=0;
 UPDATE message_hour_counts SET count=count-1 WHERE hour=CAST(OLD.created/3600 AS INTEGER)*3600;
 DELETE FROM message_hour_counts WHERE hour=CAST(OLD.created/3600 AS INTEGER)*3600 AND count<=0;
END;
"""

SCHEMA_V2 = """
ALTER TABLE groups ADD COLUMN intelligence_mode TEXT;
CREATE INDEX messages_content ON messages(content_key);
CREATE INDEX messages_created ON messages(created);
""" + MESSAGE_STATS_SCHEMA + """
CREATE TABLE learning_sizes (
 group_key TEXT PRIMARY KEY, evidence_count INTEGER NOT NULL DEFAULT 0,
 episode_count INTEGER NOT NULL DEFAULT 0);
INSERT INTO learning_sizes(group_key) SELECT key FROM groups;
CREATE TABLE learning_state (
 group_key TEXT PRIMARY KEY, epoch INTEGER NOT NULL DEFAULT 0,
 segment INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
 tail TEXT NOT NULL DEFAULT '[]', graph_checked REAL NOT NULL DEFAULT 0);
CREATE TABLE utterances (
 group_key TEXT NOT NULL, content_key TEXT NOT NULL, payload TEXT NOT NULL,
 plain TEXT NOT NULL, normalized TEXT NOT NULL, words TEXT NOT NULL,
 media INTEGER NOT NULL, updated REAL NOT NULL, PRIMARY KEY(group_key,content_key));
CREATE INDEX utterances_text ON utterances(group_key,normalized,media);
CREATE TABLE utterance_grams (
 group_key TEXT NOT NULL, gram TEXT NOT NULL, content_key TEXT NOT NULL,
 PRIMARY KEY(group_key,gram,content_key));
CREATE TABLE learning_evidence (
 id INTEGER PRIMARY KEY, group_key TEXT NOT NULL, event_key TEXT NOT NULL UNIQUE,
 platform_id TEXT, author TEXT NOT NULL, created REAL NOT NULL, content_key TEXT NOT NULL,
 history TEXT NOT NULL, reply_target TEXT, segment INTEGER NOT NULL, rule_version INTEGER NOT NULL);
CREATE INDEX evidence_group ON learning_evidence(group_key,id DESC);
CREATE INDEX evidence_time ON learning_evidence(created);
CREATE INDEX evidence_message ON learning_evidence(group_key,platform_id);
CREATE INDEX evidence_content ON learning_evidence(group_key,content_key);
CREATE TABLE evidence_links (
 evidence_id INTEGER NOT NULL, group_key TEXT NOT NULL, context TEXT NOT NULL,
 depth INTEGER NOT NULL, prompt TEXT NOT NULL, answer TEXT NOT NULL, actor TEXT NOT NULL,
 time_bucket INTEGER NOT NULL, base_weight REAL NOT NULL, created REAL NOT NULL,
 PRIMARY KEY(evidence_id,context));
CREATE INDEX links_support ON evidence_links(group_key,context,answer,actor,time_bucket);
CREATE INDEX links_answer ON evidence_links(group_key,answer);
CREATE TABLE evidence_support (
 group_key TEXT NOT NULL, context TEXT NOT NULL, answer TEXT NOT NULL, actor TEXT NOT NULL,
 time_bucket INTEGER NOT NULL, weight REAL NOT NULL, raw_count INTEGER NOT NULL,
 updated REAL NOT NULL, PRIMARY KEY(group_key,context,answer,actor,time_bucket));
CREATE TABLE reply_edges (
 group_key TEXT NOT NULL, prompt TEXT NOT NULL, answer TEXT NOT NULL,
 raw_count INTEGER NOT NULL, effective_count REAL NOT NULL,
 independent_count INTEGER NOT NULL, updated REAL NOT NULL, PRIMARY KEY(group_key,prompt,answer));
CREATE INDEX edges_answer ON reply_edges(group_key,answer,prompt);
CREATE TABLE context_counts (
 group_key TEXT NOT NULL, context TEXT NOT NULL, depth INTEGER NOT NULL,
 prompt TEXT NOT NULL, answer TEXT NOT NULL, raw_count INTEGER NOT NULL,
 effective_count REAL NOT NULL, independent_count INTEGER NOT NULL, updated REAL NOT NULL,
 PRIMARY KEY(group_key,context,answer));
CREATE INDEX contexts_prompt ON context_counts(group_key,prompt,depth);
CREATE TABLE expression_neighbors (
 group_key TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL,
 similarity REAL NOT NULL, evidence TEXT NOT NULL, epoch INTEGER NOT NULL,
 revision INTEGER NOT NULL, updated REAL NOT NULL, PRIMARY KEY(group_key,source,target));
CREATE TABLE graph_dirty (
 group_key TEXT NOT NULL, node TEXT NOT NULL, revision INTEGER NOT NULL,
 PRIMARY KEY(group_key,node));
CREATE TABLE reply_episodes (
 id TEXT PRIMARY KEY, group_key TEXT NOT NULL, created REAL NOT NULL,
 sent_at REAL, deadline REAL NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 observable INTEGER NOT NULL DEFAULT 1, settled INTEGER NOT NULL DEFAULT 0,
 mode TEXT NOT NULL, trigger_key TEXT NOT NULL, trigger_payload TEXT NOT NULL,
 trigger_id TEXT NOT NULL, previous_key TEXT NOT NULL, history TEXT NOT NULL,
 raw_answer_key TEXT NOT NULL, answer_key TEXT NOT NULL, payload TEXT NOT NULL,
 source TEXT NOT NULL, decision_json TEXT NOT NULL, connection_id TEXT NOT NULL,
 policy_version INTEGER NOT NULL, data_version INTEGER NOT NULL,
 participants INTEGER NOT NULL DEFAULT 0, reward REAL NOT NULL DEFAULT 0);
CREATE INDEX episodes_group ON reply_episodes(group_key,created DESC);
CREATE INDEX episodes_pending ON reply_episodes(settled,deadline);
CREATE INDEX episodes_context ON reply_episodes(group_key,trigger_key,previous_key,answer_key,settled);
CREATE INDEX episodes_connection ON reply_episodes(connection_id,settled);
CREATE TABLE delivery_receipts (
 id INTEGER PRIMARY KEY, episode_id TEXT NOT NULL, group_key TEXT NOT NULL,
 message_id TEXT, payload TEXT NOT NULL, created REAL NOT NULL,
 UNIQUE(group_key,message_id));
CREATE INDEX receipts_episode ON delivery_receipts(episode_id);
CREATE TABLE feedback_events (
 id INTEGER PRIMARY KEY, episode_id TEXT NOT NULL, group_key TEXT NOT NULL,
 target_id TEXT NOT NULL, actor TEXT NOT NULL, kind TEXT NOT NULL, event_key TEXT NOT NULL,
 active INTEGER NOT NULL, eligible INTEGER NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
 UNIQUE(group_key,target_id,actor,kind,event_key));
CREATE INDEX feedback_episode ON feedback_events(episode_id,eligible,active,actor);
CREATE TABLE feedback_stats (
 group_key TEXT NOT NULL, trigger_key TEXT NOT NULL, previous_key TEXT NOT NULL,
 answer_key TEXT NOT NULL, exposures INTEGER NOT NULL, reward REAL NOT NULL,
 positive_episodes INTEGER NOT NULL, PRIMARY KEY(group_key,trigger_key,previous_key,answer_key));
PRAGMA user_version=2;
"""

NEW_TABLES = (
    "learning_state", "learning_sizes", "message_group_counts", "message_content_counts", "message_hour_counts",
    "utterances", "utterance_grams", "learning_evidence", "evidence_links",
    "evidence_support", "reply_edges", "context_counts", "expression_neighbors", "graph_dirty",
    "reply_episodes", "delivery_receipts", "feedback_events", "feedback_stats",
)


class LearningStore:
    """All helpers prefixed with '_' run synchronously on the owning DB thread."""

    @staticmethod
    def _state(db, key):
        db.execute("INSERT OR IGNORE INTO learning_state(group_key) VALUES(?)", (key,))
        db.execute("INSERT OR IGNORE INTO learning_sizes(group_key) VALUES(?)", (key,))
        return db.execute("SELECT * FROM learning_state WHERE group_key=?", (key,)).fetchone()

    @staticmethod
    def _utterance(db, key, content, now):
        inserted = db.execute("""INSERT OR IGNORE INTO utterances VALUES(?,?,?,?,?,?,?,?)""",
                              (key, content.key, content.dump(), content.plain, normalize(content.plain), content.words, content.media, now)).rowcount
        if inserted and not content.media:
            db.executemany("INSERT OR IGNORE INTO utterance_grams VALUES(?,?,?)", ((key, gram, content.key) for gram in grams(content.plain)))
        return inserted

    @staticmethod
    def _delta(db, key, context, depth, prompt, answer, raw, weight, independent, updated):
        db.execute("""INSERT INTO context_counts VALUES(?,?,?,?,?,?,?,?,?)
          ON CONFLICT(group_key,context,answer) DO UPDATE SET
          raw_count=raw_count+excluded.raw_count, effective_count=MAX(0,effective_count+excluded.effective_count),
          independent_count=independent_count+excluded.independent_count,updated=MAX(updated,excluded.updated)""",
                   (key, context, depth, prompt, answer, raw, weight, independent, updated))
        if depth == 1:
            db.execute("""INSERT INTO reply_edges VALUES(?,?,?,?,?,?,?)
              ON CONFLICT(group_key,prompt,answer) DO UPDATE SET
              raw_count=raw_count+excluded.raw_count,effective_count=MAX(0,effective_count+excluded.effective_count),
              independent_count=independent_count+excluded.independent_count,updated=MAX(updated,excluded.updated)""",
                       (key, prompt, answer, raw, weight, independent, updated))

    def _link(self, db, evidence_id, key, keys, answer, actor, now, weight):
        context = packed(keys)
        bucket = int(now // INDEPENDENCE_SECONDS)
        args = (key, context, answer, actor, bucket)
        previous = db.execute("SELECT weight FROM evidence_support WHERE group_key=? AND context=? AND answer=? AND actor=? AND time_bucket=?", args).fetchone()
        db.execute("INSERT INTO evidence_links VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (evidence_id, key, context, len(keys), keys[-1], answer, actor, bucket, weight, now))
        db.execute("""INSERT INTO evidence_support VALUES(?,?,?,?,?,?,1,?)
          ON CONFLICT(group_key,context,answer,actor,time_bucket) DO UPDATE SET
          weight=MAX(weight,excluded.weight),raw_count=raw_count+1,updated=MAX(updated,excluded.updated)""", (*args, weight, now))
        delta = max(0., weight - previous[0]) if previous else weight
        self._delta(db, key, context, len(keys), keys[-1], answer, 1, delta, int(previous is None), now)

    def _remove_evidence(self, db, ids):
        if not ids:
            return {}
        encoded = packed(ids)
        sizes = db.execute("SELECT group_key,COUNT(*) FROM learning_evidence WHERE id IN (SELECT value FROM json_each(?)) GROUP BY group_key", (encoded,)).fetchall()
        links = db.execute("SELECT * FROM evidence_links WHERE evidence_id IN (SELECT value FROM json_each(?))", (encoded,)).fetchall()
        db.execute("DELETE FROM evidence_links WHERE evidence_id IN (SELECT value FROM json_each(?))", (encoded,))
        db.execute("DELETE FROM learning_evidence WHERE id IN (SELECT value FROM json_each(?))", (encoded,))
        for key, count in sizes:
            db.execute("UPDATE learning_sizes SET evidence_count=MAX(0,evidence_count-?) WHERE group_key=?", (count, key))
        affected = {(r["group_key"], r["context"], r["answer"], r["actor"], r["time_bucket"]): (r["depth"], r["prompt"]) for r in links}
        for args, (depth, prompt) in affected.items():
            old = db.execute("SELECT * FROM evidence_support WHERE group_key=? AND context=? AND answer=? AND actor=? AND time_bucket=?", args).fetchone()
            current = db.execute("SELECT COUNT(*),MAX(base_weight),MAX(created) FROM evidence_links WHERE group_key=? AND context=? AND answer=? AND actor=? AND time_bucket=?", args).fetchone()
            if not old:
                continue
            key, context, answer, _, _ = args
            self._delta(db, key, context, depth, prompt, answer, current[0] - old["raw_count"], (current[1] or 0) - old["weight"], -int(not current[0]), current[2] or 0)
            if current[0]:
                db.execute("UPDATE evidence_support SET raw_count=?,weight=?,updated=? WHERE group_key=? AND context=? AND answer=? AND actor=? AND time_bucket=?", (*current, *args))
            else:
                db.execute("DELETE FROM evidence_support WHERE group_key=? AND context=? AND answer=? AND actor=? AND time_bucket=?", args)
            db.execute("""UPDATE context_counts SET updated=COALESCE((SELECT MAX(updated) FROM evidence_support
              WHERE group_key=? AND context=? AND answer=?),0) WHERE group_key=? AND context=? AND answer=?""", (key, context, answer, key, context, answer))
            if depth == 1:
                db.execute("UPDATE reply_edges SET updated=(SELECT updated FROM context_counts WHERE group_key=? AND context=? AND answer=?) WHERE group_key=? AND prompt=? AND answer=?", (key, context, answer, key, prompt, answer))
        for key in {r[0] for r in affected}:
            db.execute("DELETE FROM context_counts WHERE group_key=? AND independent_count<=0", (key,))
            db.execute("DELETE FROM reply_edges WHERE group_key=? AND independent_count<=0", (key,))
        changed = {}
        for row in links:
            changed.setdefault(row["group_key"], set()).add(row["prompt"])
        return changed

    @staticmethod
    def _invalidate(db, key, *, cut_context=True, nodes=None):
        db.execute("UPDATE learning_state SET epoch=epoch+1,revision=revision+1 WHERE group_key=?", (key,))
        if cut_context:
            db.execute("UPDATE learning_state SET tail='[]',segment=segment+1 WHERE group_key=?", (key,))
        if nodes is None:
            db.execute("DELETE FROM expression_neighbors WHERE group_key=?", (key,))
            db.execute("""INSERT INTO graph_dirty SELECT group_key,prompt,1 FROM reply_edges WHERE group_key=? GROUP BY prompt
              ON CONFLICT(group_key,node) DO UPDATE SET revision=revision+1""", (key,))
        elif nodes:
            encoded = packed(sorted(nodes))
            dependent = {r[0] for r in db.execute("SELECT source FROM expression_neighbors WHERE group_key=? AND target IN (SELECT value FROM json_each(?))", (key, encoded))}
            db.execute("DELETE FROM expression_neighbors WHERE group_key=? AND (source IN (SELECT value FROM json_each(?)) OR target IN (SELECT value FROM json_each(?)))", (key, encoded, encoded))
            db.executemany("INSERT INTO graph_dirty VALUES(?,?,1) ON CONFLICT(group_key,node) DO UPDATE SET revision=revision+1", ((key, node) for node in nodes | dependent))

    async def history(self, key, now):
        def run(db):
            state = db.execute("SELECT * FROM learning_state WHERE group_key=?", (key,)).fetchone()
            if not state:
                return [], 0
            tail = json.loads(state["tail"])
            if tail and now - tail[-1]["created"] > SEGMENT_SECONDS:
                return [], state["revision"]
            return tail, state["revision"]
        return await self.read(run)

    async def quoted_human(self, key, message_id, now):
        def run(db):
            row = db.execute("""SELECT e.*,u.payload FROM learning_evidence e
              JOIN utterances u ON u.group_key=e.group_key AND u.content_key=e.content_key
              WHERE e.group_key=? AND e.platform_id=? AND e.created>=? ORDER BY e.id DESC LIMIT 1""", (key, str(message_id), now - 86400)).fetchone()
            return dict(row) if row else None
        return await self.read(run)

    async def observe(self, event, content, history, now, *, mode="auto", quoted=None):
        def run(db):
            with db:
                if not db.execute("INSERT OR IGNORE INTO events VALUES(?,?)", (event.event_key, now)).rowcount:
                    return False
                if db.execute("SELECT 1 FROM learning_evidence WHERE event_key=?", (event.event_key,)).fetchone():
                    return False
                # Quoted bodies have their own evidence; never invent a legacy adjacent relation.
                if quoted is None:
                    self._learn(db, event.group_key, content, event.author, event.event_key, now)
                else:
                    db.execute("UPDATE groups SET barrier=1 WHERE key=?", (event.group_key,))
                if mode == "off":
                    return True
                key = event.group_key
                state = self._state(db, key)
                if db.execute("SELECT 1 FROM bans WHERE group_key=? AND ((kind='content' AND value=?) OR (kind='keyword' AND value=?))", (key, content.key, content.words)).fetchone():
                    db.execute("UPDATE learning_state SET tail='[]',segment=segment+1 WHERE group_key=?", (key,))
                    db.execute("UPDATE groups SET barrier=1 WHERE key=?", (key,))
                    return True
                tail = json.loads(state["tail"])
                if not history or tail and now - tail[-1]["created"] > SEGMENT_SECONDS:
                    if tail:
                        db.execute("UPDATE learning_state SET segment=segment+1 WHERE group_key=?", (key,))
                    history_keys = []
                else:
                    history_keys = list(history)
                self._utterance(db, key, content, now)
                evidence_id = db.execute("""INSERT INTO learning_evidence(group_key,event_key,platform_id,author,created,content_key,history,reply_target,segment,rule_version)
                  VALUES(?,?,?,?,?,?,?,?,?,?)""", (key, event.event_key, getattr(event, "message_id", None), event.author, now, content.key,
                  packed(history_keys), str(event.reply_to) if getattr(event, "reply_to", None) else None, state["segment"], POLICY_VERSION)).lastrowid
                size = db.execute("UPDATE learning_sizes SET evidence_count=evidence_count+1 WHERE group_key=? RETURNING evidence_count", (key,)).fetchone()[0]
                if quoted:
                    keys = [quoted["content_key"]]
                    self._link(db, evidence_id, key, keys, content.key, event.author, now, 1.)
                    if history_keys and history_keys[-1]["event_key"] == quoted["event_key"]:
                        for depth in (2, 3):
                            if len(history_keys) >= depth:
                                self._link(db, evidence_id, key, [r["content_key"] for r in history_keys[-depth:]], content.key, event.author, now, 1.)
                else:
                    for depth in (1, 2, 3):
                        if len(history_keys) >= depth:
                            self._link(db, evidence_id, key, [r["content_key"] for r in history_keys[-depth:]], content.key, event.author, now, .5)
                item = {"event_key": event.event_key, "content_key": content.key, "created": now}
                tail = (history_keys + [item])[-3:]
                db.execute("UPDATE learning_state SET tail=?,revision=revision+1 WHERE group_key=?", (packed(tail), key))
                if history_keys or quoted:
                    prompt = quoted["content_key"] if quoted else history_keys[-1]["content_key"]
                    db.execute("""INSERT INTO graph_dirty VALUES(?,?,1) ON CONFLICT(group_key,node) DO UPDATE SET revision=revision+1""", (key, prompt))
                    # New edges can change neighborhoods of other expressions sharing this answer.
                    db.execute("""INSERT INTO graph_dirty SELECT group_key,prompt,1 FROM reply_edges
                      WHERE group_key=? AND answer=? LIMIT ?
                      ON CONFLICT(group_key,node) DO UPDATE SET revision=revision+1""", (key, content.key, POLICY.reverse_limit))
                excess = [r[0] for r in db.execute("SELECT id FROM learning_evidence WHERE group_key=? ORDER BY id LIMIT ?", (key, size - self.config.max_evidence_per_group))] if size > self.config.max_evidence_per_group else []
                if excess:
                    changed = self._remove_evidence(db, excess)
                    self._invalidate(db, key, cut_context=False, nodes=changed.get(key, set()))
                return True
        return await self.call(run)

    def _prune_learning(self, db, now):
        rows = db.execute("SELECT id,group_key FROM learning_evidence WHERE created<? ORDER BY created LIMIT 1000", (now - self.config.evidence_retention_days * 86400,)).fetchall()
        changed = self._remove_evidence(db, [r["id"] for r in rows])
        for key in {r["group_key"] for r in rows}:
            self._invalidate(db, key, cut_context=False, nodes=changed.get(key, set()))
        episodes = db.execute("SELECT id,group_key FROM reply_episodes WHERE created<? ORDER BY created LIMIT 500", (now - self.config.feedback_retention_days * 86400,)).fetchall()
        ids = [r[0] for r in episodes]
        self._remove_episodes(db, ids)
        for row in episodes:
            self._gc_epochs.pop(row["group_key"], None)

    async def collect_utterances(self):
        # Reference discovery is expensive; do it on the inspection connection,
        # then validate the revision before a short deletion transaction.
        states = await self.read(lambda db: [dict(r) for r in db.execute("SELECT group_key,epoch,revision FROM learning_state ORDER BY group_key")])
        cursor = getattr(self, "_gc_cursor", "")
        states.sort(key=lambda r: (r["group_key"] <= cursor, r["group_key"]))
        state = next((r for r in states if self._gc_epochs.get(r["group_key"]) != r["epoch"]), None)
        if not state:
            return
        key = state["group_key"]
        self._gc_cursor = key
        def discover(db):
            return [r[0] for r in db.execute("""WITH live(content_key) AS (
              SELECT content_key FROM learning_evidence WHERE group_key=?
              UNION SELECT json_extract(j.value,'$.content_key') FROM learning_evidence e,json_each(e.history) j WHERE e.group_key=?
              UNION SELECT prompt FROM reply_edges WHERE group_key=?
              UNION SELECT answer FROM reply_edges WHERE group_key=?
              UNION SELECT trigger_key FROM reply_episodes WHERE group_key=?
              UNION SELECT answer_key FROM reply_episodes WHERE group_key=?
              UNION SELECT json_extract(j.value,'$.content_key') FROM reply_episodes p,json_each(p.history) j WHERE p.group_key=?
              ) SELECT content_key FROM utterances WHERE group_key=? AND content_key NOT IN live""", (key,) * 8)]
        obsolete = await self.inspect(discover, timeout=3.)
        if not obsolete:
            self._gc_epochs[key] = state["epoch"]
            return
        def remove(db):
            with db:
                current = db.execute("SELECT epoch,revision FROM learning_state WHERE group_key=?", (key,)).fetchone()
                if not current or tuple(current) != (state["epoch"], state["revision"]):
                    return False
                for table in ("utterance_grams", "utterances"):
                    db.execute(f"DELETE FROM {table} WHERE group_key=? AND content_key IN (SELECT value FROM json_each(?))", (key, packed(obsolete)))
                return True
        if await self.call(remove):
            self._gc_epochs[key] = state["epoch"]

    async def rebuild_learning(self, key):
        """Reinterpret retained facts with the current rule version, transactionally."""
        def run(db):
            with db:
                for table in ("evidence_links", "evidence_support", "reply_edges", "context_counts"):
                    db.execute(f"DELETE FROM {table} WHERE group_key=?", (key,))
                for row in db.execute("SELECT * FROM learning_evidence WHERE group_key=? ORDER BY id", (key,)).fetchall():
                    history = json.loads(row["history"])
                    quote = db.execute("SELECT * FROM learning_evidence WHERE group_key=? AND platform_id=? AND created BETWEEN ? AND ? LIMIT 1", (key, row["reply_target"], row["created"] - 86400, row["created"])).fetchone() if row["reply_target"] else None
                    if row["reply_target"]:
                        if quote:
                            self._link(db, row["id"], key, [quote["content_key"]], row["content_key"], row["author"], row["created"], 1.)
                        if not quote or not history or history[-1]["event_key"] != quote["event_key"]:
                            continue
                        depths, weight = (2, 3), 1.
                    else:
                        depths, weight = (1, 2, 3), .5
                    for depth in depths:
                        if len(history) >= depth:
                            self._link(db, row["id"], key, [r["content_key"] for r in history[-depth:]], row["content_key"], row["author"], row["created"], weight)
                db.execute("UPDATE learning_evidence SET rule_version=? WHERE group_key=?", (POLICY_VERSION, key))
                self._invalidate(db, key)
        await self.call(run)
