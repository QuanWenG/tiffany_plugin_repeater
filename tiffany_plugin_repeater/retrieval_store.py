"""Bounded, bulk reads for candidate selection; no per-candidate SQL."""
import json

from .context import POLICY, grams, lexical_similarity, normalize, packed
from .models import GroupSettings


class RetrievalStore:
    async def selection_bundle(self, key, limit):
        def run(db):
            rows = db.execute("""SELECT 'settings' AS kind,json_object('enabled',enabled,'text_enabled',text_enabled,
              'expression_enabled',expression_enabled,'media_enabled',media_enabled,'preset',preset,
              'intelligence_mode',intelligence_mode) AS value FROM groups WHERE key=?
              UNION ALL SELECT 'recent',json_object('author',author,'content_key',content_key,'payload',payload,'words',words,'created',created)
              FROM (SELECT * FROM messages WHERE group_key=? ORDER BY id DESC LIMIT ?)
              UNION ALL SELECT 'sent',json_object('content_key',content_key,'payload',payload,'words',words)
              FROM (SELECT * FROM sent WHERE group_key=? ORDER BY id DESC LIMIT ?)
              UNION ALL SELECT 'ban',json_object('kind',kind,'value',value) FROM bans WHERE group_key=?
              UNION ALL SELECT 'state',json_object('revision',revision,'epoch',epoch) FROM learning_state WHERE group_key=?""",
                              (key, key, limit, key, limit, key, key)).fetchall()
            result = {"recent": [], "sent": [], "ban": [], "revision": 0, "epoch": 0}
            result["settings"] = GroupSettings(enabled=self.config.enabled_default)
            for row in rows:
                value = json.loads(row["value"])
                if row["kind"] == "settings":
                    result["settings"] = GroupSettings(**value)
                elif row["kind"] == "state":
                    result.update(value)
                else:
                    result[row["kind"]].append(value)
            return result
        return await self.read(run)

    async def intelligence_bundle(self, key, prompt, history, *, threshold=POLICY.direct_support):
        def run(db):
            normalized = normalize(prompt.plain)
            anchors = {r["content_key"]: 1. for r in db.execute("SELECT content_key FROM utterances WHERE group_key=? AND normalized=? AND media=0 ORDER BY content_key LIMIT ?", (key, normalized, POLICY.anchors))}
            # Exact anchors with insufficient evidence must not hide a useful near match.
            if anchors:
                supported = db.execute("SELECT 1 FROM reply_edges WHERE group_key=? AND prompt IN (SELECT value FROM json_each(?)) AND independent_count>=? AND effective_count>=? LIMIT 1", (key, packed(list(anchors)), max(POLICY.direct_support, threshold), POLICY.effective_support)).fetchone()
            else:
                supported = False
            if not supported:
                candidates = db.execute("""SELECT u.content_key,u.plain,COUNT(*) AS overlap
                  FROM utterance_grams g JOIN utterances u USING(group_key,content_key)
                  WHERE g.group_key=? AND g.gram IN (SELECT value FROM json_each(?))
                  GROUP BY g.content_key ORDER BY overlap DESC,g.content_key LIMIT 200""", (key, packed(sorted(grams(prompt.plain))))).fetchall()
                scored = [(r["content_key"], lexical_similarity(prompt.plain, r["plain"])) for r in candidates]
                anchors = dict(sorted(((k, s) for k, s in scored if s), key=lambda item: (-item[1], item[0]))[:POLICY.anchors])
            query = [r["content_key"] for r in history[-2:]] + [prompt.key]
            near_rows = db.execute("""SELECT n.* FROM expression_neighbors n JOIN learning_state s ON s.group_key=n.group_key
              WHERE n.group_key=? AND n.source IN (SELECT value FROM json_each(?)) AND n.similarity>=?
              AND (SELECT COUNT(*) FROM (SELECT e.prompt FROM reply_edges e
                JOIN utterances u ON u.group_key=e.group_key AND u.content_key=e.prompt AND u.media=0
                WHERE e.group_key=? GROUP BY e.prompt LIMIT ?))>=?""", (key, packed(sorted(set(anchors) | set(query))), POLICY.graph_similarity, key, POLICY.graph_minimum_nodes, POLICY.graph_minimum_nodes)).fetchall()
            neighbors = {(r["source"], r["target"]): r["similarity"] for r in near_rows}
            graph_sources = {}
            for row in near_rows:
                if row["source"] in anchors and row["target"] not in anchors:
                    graph_sources[row["target"]] = max(graph_sources.get(row["target"], 0), anchors[row["source"]] * row["similarity"])
            sources = set(anchors) | set(graph_sources)
            rows = [dict(r) for r in db.execute("""SELECT * FROM context_counts WHERE group_key=?
              AND prompt IN (SELECT value FROM json_each(?)) AND independent_count>=? AND effective_count>=?
              ORDER BY effective_count DESC,context,answer LIMIT 2000""", (key, packed(sorted(sources)), POLICY.direct_support, POLICY.effective_support))]
            keys = set(query) | sources | {r["answer"] for r in rows}
            for row in rows:
                keys.update(json.loads(row["context"]))
            utterances = {r["content_key"]: dict(r) for r in db.execute("SELECT * FROM utterances WHERE group_key=? AND content_key IN (SELECT value FROM json_each(?))", (key, packed(sorted(keys))))}
            texts = {k: r["plain"] for k, r in utterances.items()}
            texts[prompt.key] = prompt.plain
            previous = history[-1]["content_key"] if history else ""
            feedback = self._feedback_weights(db, key, prompt.key, previous)
            experience = self._experience(db, key, prompt.key, previous)
            return {"anchors": anchors, "graph_sources": graph_sources, "neighbors": neighbors,
                    "rows": rows, "utterances": utterances, "texts": texts, "query": query,
                    "feedback": feedback, "experience": experience}
        return await self.read(run)

    async def graph_snapshot(self, limit=POLICY.graph_batch):
        def run(db):
            state = db.execute("""SELECT s.* FROM learning_state s JOIN groups g ON g.key=s.group_key
              WHERE g.enabled=1 AND COALESCE(g.intelligence_mode,?)!='off' AND EXISTS
              (SELECT 1 FROM graph_dirty d WHERE d.group_key=s.group_key)
              ORDER BY s.graph_checked,s.group_key LIMIT 1""", (self.config.intelligence_mode,)).fetchone()
            if not state:
                return None
            key = state["group_key"]
            dirty = [dict(r) for r in db.execute("SELECT * FROM graph_dirty WHERE group_key=? ORDER BY node LIMIT ?", (key, limit))]
            edges = [dict(r) for r in db.execute("""SELECT e.*,u.plain AS answer_text,u.media AS answer_media
              FROM reply_edges e JOIN utterances p ON p.group_key=e.group_key AND p.content_key=e.prompt AND p.media=0
              JOIN utterances u ON u.group_key=e.group_key AND u.content_key=e.answer WHERE e.group_key=?""", (key,))]
            return {"state": dict(state), "dirty": dirty, "edges": edges}
        return await self.read(run)

    async def commit_neighbors(self, snapshot, results, now):
        def run(db):
            with db:
                key = snapshot["state"]["group_key"]
                state = db.execute("SELECT epoch FROM learning_state WHERE group_key=?", (key,)).fetchone()
                if not state or state[0] != snapshot["state"]["epoch"]:
                    return False
                db.execute("UPDATE learning_state SET graph_checked=? WHERE group_key=?", (now, key))
                versions = {r["node"]: r["revision"] for r in snapshot["dirty"]}
                for node, neighbors in results.items():
                    db.execute("DELETE FROM expression_neighbors WHERE group_key=? AND source=?", (key, node))
                    db.executemany("INSERT INTO expression_neighbors VALUES(?,?,?,?,?,?,?,?)", ((key, node, target, score, packed(evidence), state[0], snapshot["state"]["revision"], now) for target, score, evidence in neighbors))
                    db.execute("DELETE FROM graph_dirty WHERE group_key=? AND node=? AND revision=?", (key, node, versions[node]))
                return True
        return await self.call(run)
