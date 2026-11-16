"""Durable delivery attribution and bounded, reversible interaction statistics."""
import uuid
from collections import Counter

from .context import POLICY, packed


def reward_for(participants):
    return POLICY.rewards[min(len(POLICY.rewards) - 1, participants)]


class FeedbackStore:
    @staticmethod
    def _scored(row):
        return row["settled"] and row["observable"] and row["status"] == "sent" and row["mode"] == "normal"

    @staticmethod
    def _stat_delta(db, row, n, reward, positive):
        db.execute("""INSERT INTO feedback_stats VALUES(?,?,?,?,?,?,?)
          ON CONFLICT(group_key,trigger_key,previous_key,answer_key) DO UPDATE SET
          exposures=exposures+excluded.exposures,reward=MAX(0,reward+excluded.reward),
          positive_episodes=positive_episodes+excluded.positive_episodes""",
                   (row["group_key"], row["trigger_key"], row["previous_key"], row["answer_key"], n, reward, positive))
        db.execute("DELETE FROM feedback_stats WHERE group_key=? AND trigger_key=? AND previous_key=? AND answer_key=? AND exposures<=0",
                   (row["group_key"], row["trigger_key"], row["previous_key"], row["answer_key"]))

    def _refresh_feedback(self, db, episode_id):
        row = db.execute("SELECT * FROM reply_episodes WHERE id=?", (episode_id,)).fetchone()
        if row is None:
            return
        participants = db.execute("SELECT COUNT(DISTINCT actor) FROM feedback_events WHERE episode_id=? AND active=1 AND eligible=1 AND actor!=''", (episode_id,)).fetchone()[0]
        reward = reward_for(participants)
        if self._scored(row):
            self._stat_delta(db, row, 0, reward - row["reward"], int(reward > 0) - int(row["reward"] > 0))
        db.execute("UPDATE reply_episodes SET participants=?,reward=? WHERE id=?", (participants, reward, episode_id))

    def _remove_episodes(self, db, ids):
        if not ids:
            return
        encoded = packed(ids)
        rows = db.execute("SELECT * FROM reply_episodes WHERE id IN (SELECT value FROM json_each(?))", (encoded,)).fetchall()
        for row in rows:
            if self._scored(row):
                self._stat_delta(db, row, -1, -row["reward"], -int(row["reward"] > 0))
        for table in ("feedback_events", "delivery_receipts"):
            db.execute(f"DELETE FROM {table} WHERE episode_id IN (SELECT value FROM json_each(?))", (encoded,))
        db.execute("DELETE FROM reply_episodes WHERE id IN (SELECT value FROM json_each(?))", (encoded,))
        for key, count in Counter(row["group_key"] for row in rows).items():
            db.execute("UPDATE learning_sizes SET episode_count=MAX(0,episode_count-?) WHERE group_key=?", (count, key))

    async def begin_episode(self, key, decision, connection_id, now):
        def run(db):
            with db:
                return self._begin_episode(db, key, decision, connection_id, now)
        return await self.call(run)

    def _begin_episode(self, db, key, decision, connection_id, now):
        episode_id = uuid.uuid4().hex
        details = {
            "probability": decision.probability, "candidates": decision.diagnostics[:10],
            "shadow": decision.shadow, "policy_version": decision.policy_version,
            "data_version": decision.data_version,
            "source_expression": decision.source_expression, "evidence_refs": decision.evidence_refs,
        }
        contexts = [r["context"] for r in decision.evidence_refs if r.get("type") == "human"]
        if contexts:
            details["observations"] = [dict(r) for r in db.execute("""SELECT e.id,e.event_key,e.author,e.created,
              (SELECT MAX(base_weight) FROM evidence_links l WHERE l.evidence_id=e.id AND l.context IN (SELECT value FROM json_each(?))) AS base_weight
              FROM learning_evidence e WHERE e.group_key=? AND e.content_key=?
              AND EXISTS(SELECT 1 FROM evidence_links l WHERE l.evidence_id=e.id AND l.context IN (SELECT value FROM json_each(?)))
              ORDER BY e.id DESC LIMIT 10""", (packed(contexts), key, decision.raw_answer_key, packed(contexts)))]
        db.execute("""INSERT INTO reply_episodes
                  (id,group_key,created,deadline,mode,trigger_key,trigger_payload,trigger_id,previous_key,
                   history,raw_answer_key,answer_key,payload,source,decision_json,connection_id,policy_version,data_version,status,observable,settled)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (episode_id, key, now, now + self.config.feedback_window_seconds, decision.mode,
                            decision.trigger_key, decision.trigger_payload, decision.trigger_id, decision.previous_key,
                            packed(decision.history), decision.raw_answer_key or decision.content.key, decision.content.key,
                            decision.content.dump(), decision.source, packed(details), connection_id,
                            decision.policy_version, decision.data_version,
                            "shadow" if decision.mode == "shadow" else "pending", int(decision.mode != "shadow"), int(decision.mode == "shadow")))
        size = db.execute("""INSERT INTO learning_sizes(group_key,episode_count) VALUES(?,1)
          ON CONFLICT(group_key) DO UPDATE SET episode_count=episode_count+1 RETURNING episode_count""", (key,)).fetchone()[0]
        excess = [r[0] for r in db.execute("SELECT id FROM reply_episodes WHERE group_key=? ORDER BY created,id LIMIT ?", (key, size - self.config.max_reply_episodes_per_group))] if size > self.config.max_reply_episodes_per_group else []
        self._remove_episodes(db, excess)
        return episode_id

    async def add_receipt(self, episode_id, key, receipt):
        def run(db):
            with db:
                if not db.execute("SELECT 1 FROM reply_episodes WHERE id=?", (episode_id,)).fetchone():
                    return
                inserted = db.execute("INSERT OR IGNORE INTO delivery_receipts(episode_id,group_key,message_id,payload,created) VALUES(?,?,?,?,?)",
                                      (episode_id, key, receipt.message_id, receipt.content.dump(), receipt.created)).rowcount
                if not inserted or not receipt.message_id:
                    db.execute("UPDATE reply_episodes SET observable=0 WHERE id=?", (episode_id,))
                db.execute("UPDATE reply_episodes SET deadline=CASE WHEN sent_at IS NULL THEN ? ELSE deadline END,sent_at=COALESCE(sent_at,?) WHERE id=?",
                           (receipt.created + self.config.feedback_window_seconds, receipt.created, episode_id))
        await self.call(run, critical=True)

    async def finish_episode(self, episode_id, complete):
        def run(db):
            with db:
                self._finish_episode(db, episode_id, complete)
        await self.call(run)

    @staticmethod
    def _finish_episode(db, episode_id, complete):
        count = db.execute("SELECT COUNT(*) FROM delivery_receipts WHERE episode_id=?", (episode_id,)).fetchone()[0]
        status = "sent" if complete and count else "partial" if count else "failed"
        db.execute("UPDATE reply_episodes SET status=?,observable=CASE WHEN ?='sent' THEN observable ELSE 0 END WHERE id=?", (status, status, episode_id))

    async def receipt_episode(self, key, target_id):
        def run(db):
            row = db.execute("SELECT p.* FROM delivery_receipts d JOIN reply_episodes p ON p.id=d.episode_id WHERE d.group_key=? AND d.message_id=?", (key, str(target_id))).fetchone()
            return dict(row) if row else None
        return await self.read(run)

    async def record_feedback(self, key, target_id, actor, kind, event_key, active, now):
        """Reaction keys identify emoji states, quote keys identify source messages."""
        def run(db):
            with db:
                row = db.execute("SELECT p.* FROM delivery_receipts d JOIN reply_episodes p ON p.id=d.episode_id WHERE d.group_key=? AND d.message_id=?", (key, str(target_id))).fetchone()
                if not row:
                    return False
                eligible = bool(actor) and now <= row["deadline"] and now >= row["created"] and bool(active)
                # Duplicated quote messages are immutable; reactions are current states.
                if kind == "quote":
                    db.execute("INSERT OR IGNORE INTO feedback_events(episode_id,group_key,target_id,actor,kind,event_key,active,eligible,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                               (row["id"], key, str(target_id), actor, kind, event_key, int(active), int(eligible), now, now))
                else:
                    db.execute("""INSERT INTO feedback_events(episode_id,group_key,target_id,actor,kind,event_key,active,eligible,created,updated)
                      VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(group_key,target_id,actor,kind,event_key) DO UPDATE SET
                      eligible=CASE WHEN excluded.active=0 THEN feedback_events.eligible
                        WHEN feedback_events.active=1 THEN feedback_events.eligible ELSE excluded.eligible END,
                      active=excluded.active,updated=excluded.updated""",
                               (row["id"], key, str(target_id), actor, kind, event_key, int(active), int(eligible), now, now))
                self._refresh_feedback(db, row["id"])
                return True
        return await self.call(run)

    async def settle_feedback(self, now):
        def run(db):
            with db:
                rows = db.execute("SELECT * FROM reply_episodes WHERE settled=0 AND deadline<=? LIMIT 500", (now,)).fetchall()
                for row in rows:
                    db.execute("UPDATE reply_episodes SET settled=1 WHERE id=?", (row["id"],))
                    if row["observable"] and row["status"] == "sent" and row["mode"] == "normal":
                        self._stat_delta(db, row, 1, row["reward"], int(row["reward"] > 0))
                return len(rows)
        return await self.call(run)

    async def invalidate_observations(self, connection_id=None, key=None):
        def run(db):
            with db:
                clauses, args = ["settled=0"], []
                if connection_id is not None:
                    clauses.append("connection_id=?")
                    args.append(connection_id)
                if key is not None:
                    clauses.append("group_key=?")
                    args.append(key)
                db.execute("UPDATE reply_episodes SET observable=0 WHERE " + " AND ".join(clauses), args)
        await self.call(run, critical=True)

    async def feedback_weights(self, key, trigger_key, previous_key):
        return await self.read(lambda db: self._feedback_weights(db, key, trigger_key, previous_key))

    @staticmethod
    def _feedback_weights(db, key, trigger_key, previous_key):
        return {r["answer_key"]: 1 + POLICY.feedback_gain * r["reward"] / (r["exposures"] + POLICY.feedback_prior)
                for r in db.execute("SELECT * FROM feedback_stats WHERE group_key=? AND trigger_key=? AND previous_key=? AND exposures>=? AND positive_episodes>=?", (key, trigger_key, previous_key, POLICY.feedback_exposures, POLICY.feedback_positives))}

    @staticmethod
    def _experience(db, key, trigger_key, previous_key):
        return [dict(r) for r in db.execute("""SELECT p.answer_key,p.payload,COUNT(DISTINCT p.id) AS successes,
          COUNT(DISTINCT f.actor) AS people,MAX(p.created) AS updated,json_group_array(DISTINCT p.id) AS episode_ids
          FROM reply_episodes p JOIN feedback_events f ON f.episode_id=p.id
          WHERE p.group_key=? AND p.trigger_key=? AND p.previous_key=? AND p.mode='normal'
          AND p.status='sent' AND p.observable=1 AND p.settled=1 AND p.reward>0
          AND f.active=1 AND f.eligible=1 AND f.actor!=''
          GROUP BY p.answer_key HAVING COUNT(DISTINCT p.id)>=? AND COUNT(DISTINCT f.actor)>=?
          ORDER BY successes DESC,p.answer_key LIMIT ?""", (key, trigger_key, previous_key, POLICY.experience_episodes, POLICY.experience_people, POLICY.candidates))]
