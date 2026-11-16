"""Optional loopback HTTP service with authenticated, read-only corpus queries."""
import hmac
import json
import time
from pathlib import Path

from aiohttp import web

from .models import Content
from .storage import QueryTimeout, StorageBusy


class Dashboard:
    def __init__(self, service):
        self.service = service
        self.runner = None
        self.port = None

    async def open(self):
        app = web.Application(middlewares=[self.authenticate])
        app.router.add_get("/", self.index)
        app.router.add_get("/api/{view:overview|messages|relations|sent|groups|detail|evidence|neighbors|decisions|feedback}", self.api)
        self.runner = web.AppRunner(app, access_log=None)
        try:
            await self.runner.setup()
            config = self.service.config
            site = web.TCPSite(self.runner, config.dashboard_host, config.dashboard_port)
            await site.start()
            self.port = self.runner.addresses[0][1]
        except BaseException:
            await self.close()
            raise

    async def close(self):
        if self.runner:
            await self.runner.cleanup()
            self.runner = None

    @web.middleware
    async def authenticate(self, request, handler):
        if request.path.startswith("/api/"):
            supplied = request.headers.get("Authorization", "")
            expected = "Bearer " + self.service.config.dashboard_token
            if not hmac.compare_digest(supplied.encode(), expected.encode()):
                raise web.HTTPUnauthorized(text="需要有效的访问令牌")
        response = await handler(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'"
        return response

    async def index(self, request):
        return web.Response(text=Path(__file__).with_name("web").joinpath("index.html").read_text("utf-8"), content_type="text/html")

    async def api(self, request):
        try:
            result = await self.query(request.match_info["view"], request.query)
        except (ValueError, KeyError, TypeError):
            raise web.HTTPBadRequest(text="查询参数无效") from None
        except (StorageBusy, QueryTimeout) as exc:
            raise web.HTTPServiceUnavailable(text=str(exc)) from None
        return web.json_response(result)

    async def query(self, view, query):
        service = self.service
        if view in ("evidence", "neighbors", "decisions", "feedback"):
            return await self.learning_query(view, query)
        if view == "overview":
            def overview(db):
                counts = {table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("relations", "sent", "groups")}
                counts["messages"] = db.execute("SELECT COALESCE(SUM(count),0) FROM message_group_counts").fetchone()[0]
                counts["enabled_groups"] = db.execute("SELECT COUNT(*) FROM groups WHERE enabled=1").fetchone()[0]
                counts["contents"] = db.execute("SELECT COUNT(*) FROM message_content_counts").fetchone()[0]
                cutoff = time.time() - 86400
                boundary = int(cutoff // 3600) * 3600
                counts["last_24h"] = [dict(r) for r in db.execute("SELECT hour,count FROM message_hour_counts WHERE hour>? ORDER BY hour", (boundary,))]
                partial = db.execute("SELECT COUNT(*) FROM messages WHERE created>? AND created<?", (cutoff, boundary + 3600)).fetchone()[0]
                if partial:
                    counts["last_24h"].insert(0, {"hour": boundary, "count": partial})
                counts["learning"] = {table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("reply_edges", "expression_neighbors", "graph_dirty")}
                counts["learning"]["learning_evidence"] = db.execute("SELECT COALESCE(SUM(evidence_count),0) FROM learning_sizes").fetchone()[0]
                counts["answers"] = [dict(r) for r in db.execute("""SELECT mode,status,observable,settled,COUNT(*) AS count,
                  SUM(participants) AS participants,SUM(reward>0) AS interacted FROM reply_episodes
                  GROUP BY mode,status,observable,settled""")]
                counts["sources"] = [dict(r) for r in db.execute("SELECT source,COUNT(*) AS count FROM reply_episodes WHERE status='sent' GROUP BY source")]
                counts["concentration"] = [dict(r) for r in db.execute("SELECT answer_key,COUNT(*) AS count FROM reply_episodes WHERE status='sent' GROUP BY answer_key ORDER BY count DESC,answer_key LIMIT 10")]
                return counts
            result = await service.store.inspect(overview)
            result["runtime"] = {"active": service.active, "pending_sends": len(service.pending), "sober_timers": len(service.sober_tasks)}
            times = sorted(service.engine.selection_times)
            result["runtime"]["selection_p95_ms"] = round(times[min(len(times) - 1, int(len(times) * .95))] * 1000, 2) if times else None
            result["runtime"]["metrics"] = dict(service.store.metrics)
            result["runtime"]["buffered_feedback"] = len(service.feedback.pending)
            return result
        if view == "detail":
            table = query.get("table", "messages")
            if table in ("reply_episodes", "learning_evidence"):
                return await self.learning_detail(table, query["id"])
            if table not in ("messages", "sent"):
                raise ValueError("invalid detail table")
            identifier = int(query["id"])
            def detail(db):
                row = db.execute(f"SELECT * FROM {table} WHERE id=?", (identifier,)).fetchone()
                if row is None:
                    return {"record": None, "sources": []}
                record = dict(row)
                sources = [dict(r) for r in db.execute("SELECT id,group_key,author,event_key,created FROM messages WHERE content_key=? ORDER BY id DESC LIMIT 100", (row["content_key"],))]
                record["content"] = self.content(record.pop("payload"))
                return {"record": record, "sources": sources}
            return await service.store.inspect(detail)
        if view not in ("messages", "relations", "sent", "groups"):
            raise ValueError("unknown view")
        page, limit = int(query.get("page", "1")), int(query.get("limit", "50"))
        if not 1 <= page <= 1000000 or not 1 <= limit <= 100:
            raise ValueError("invalid pagination")
        if view == "relations":
            table, group_column, ordering = "variants", "group_key", "updated DESC"
        elif view == "groups":
            table, group_column, ordering = "groups", "key", "key"
        else:
            table, group_column, ordering = view, "group_key", "id DESC"
        clauses, args = [], []
        if query.get("group"):
            clauses.append(f"{group_column}=?")
            args.append(query["group"])
        text = query.get("text", "")
        if len(text) > 4000:
            raise ValueError("query too long")
        if text and view != "groups":
            expression = "content_text(payload)"
            if query.get("mode", "contains") == "exact":
                clauses.append(expression + "=?")
                args.append(text)
            else:
                pattern = "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
                clauses.append(f"({expression} LIKE ? ESCAPE '\\'" + (" OR prompt LIKE ? ESCAPE '\\')" if view == "relations" else ")"))
                args.extend([pattern, pattern] if view == "relations" else [pattern])
        if query.get("author") and view == "messages":
            clauses.append("author=?")
            args.append(query["author"])
        if query.get("media") in ("text", "media") and view in ("messages", "relations", "sent"):
            expression = "content_media(payload)" if view == "sent" else "media"
            clauses.append(expression + "=?")
            args.append(query["media"] == "media")
        for bound, operator in (("since", ">="), ("until", "<=")):
            if query.get(bound) and view != "groups":
                stamp = float(query[bound])
                if not 0 <= stamp < 1e12:
                    raise ValueError("invalid timestamp")
                clauses.append(("updated" if view == "relations" else "created") + operator + "?")
                args.append(stamp)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        def search(db):
            total = db.execute(f"SELECT COUNT(*) FROM {table}{where}", args).fetchone()[0]
            rows = [dict(r) for r in db.execute(f"SELECT * FROM {table}{where} ORDER BY {ordering} LIMIT ? OFFSET ?", (*args, limit, (page - 1) * limit))]
            for row in rows:
                if "payload" in row:
                    row["content"] = self.content(row.pop("payload"))
            return {"rows": rows, "total": total, "page": page, "pages": max(1, (total + limit - 1) // limit)}
        return await service.store.inspect(search)

    async def learning_query(self, view, query):
        page, limit = int(query.get("page", "1")), int(query.get("limit", "50"))
        if not 1 <= page <= 1000000 or not 1 <= limit <= 100:
            raise ValueError("invalid pagination")
        views = {
            "evidence": ("learning_evidence e JOIN utterances u ON u.group_key=e.group_key AND u.content_key=e.content_key", "e.*,u.payload", "e.group_key", "e.created", "e.id DESC", "content_text(u.payload)"),
            "neighbors": ("expression_neighbors n JOIN utterances s ON s.group_key=n.group_key AND s.content_key=n.source JOIN utterances t ON t.group_key=n.group_key AND t.content_key=n.target", "n.*,s.payload AS source_payload,t.payload AS target_payload", "n.group_key", "n.updated", "n.similarity DESC,n.source,n.target", "content_text(s.payload)||' '||content_text(t.payload)"),
            "decisions": ("reply_episodes p", "p.id,p.group_key,p.created,p.deadline,p.status,p.observable,p.settled,p.mode,p.source,p.trigger_payload,p.payload,p.participants,p.reward,json_extract(p.decision_json,'$.probability') AS probability", "p.group_key", "p.created", "p.created DESC,p.id", "content_text(p.payload)||' '||content_text(p.trigger_payload)"),
            "feedback": ("feedback_events f JOIN reply_episodes p ON p.id=f.episode_id", "f.*,p.payload", "f.group_key", "f.created", "f.id DESC", "content_text(p.payload)"),
        }
        table, columns, group_column, stamp_column, ordering, expression = views[view]
        clauses, args = [], []
        if query.get("group"):
            clauses.append(group_column + "=?")
            args.append(query["group"])
        if query.get("author") and view in ("evidence", "feedback"):
            clauses.append(("e.author" if view == "evidence" else "f.actor") + "=?")
            args.append(query["author"])
        if query.get("media") in ("text", "media") and view != "neighbors":
            clauses.append("content_media(" + ("u.payload" if view == "evidence" else "p.payload") + ")=?")
            args.append(query["media"] == "media")
        text = query.get("text", "")
        if len(text) > 4000:
            raise ValueError("query too long")
        if text:
            if query.get("mode") == "exact":
                clauses.append(expression + "=?")
                args.append(text)
            else:
                clauses.append("(" + expression + ") LIKE ? ESCAPE '\\'")
                args.append("%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        for bound, operator in (("since", ">="), ("until", "<=")):
            if query.get(bound):
                stamp = float(query[bound])
                if not 0 <= stamp < 1e12:
                    raise ValueError("invalid timestamp")
                clauses.append(stamp_column + operator + "?")
                args.append(stamp)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        def search(db):
            total = db.execute(f"SELECT COUNT(*) FROM {table}{where}", args).fetchone()[0]
            rows = [dict(r) for r in db.execute(f"SELECT {columns} FROM {table}{where} ORDER BY {ordering} LIMIT ? OFFSET ?", (*args, limit, (page - 1) * limit))]
            for row in rows:
                for field, name in (("payload", "content"), ("trigger_payload", "trigger"), ("source_payload", "source_content"), ("target_payload", "target_content")):
                    if field in row:
                        row[name] = self.content(row.pop(field))
                # Large histories are available on the detail route.
                row.pop("history", None)
            return {"rows": rows, "total": total, "page": page, "pages": max(1, (total + limit - 1) // limit)}
        return await self.service.store.inspect(search)

    async def learning_detail(self, table, identifier):
        identifier = int(identifier) if table == "learning_evidence" else str(identifier)
        if table == "reply_episodes" and (len(identifier) != 32 or any(c not in "0123456789abcdef" for c in identifier)):
            raise ValueError("invalid episode id")
        def detail(db):
            row = db.execute(f"SELECT * FROM {table} WHERE id=?", (identifier,)).fetchone()
            if row is None:
                return {"record": None}
            record = dict(row)
            record["history"] = json.loads(record["history"])
            if table == "reply_episodes":
                record["decision"] = json.loads(record.pop("decision_json"))
                record["content"] = self.content(record.pop("payload"))
                record["trigger"] = self.content(record.pop("trigger_payload")) if record["trigger_payload"] else None
                receipts = [dict(r) for r in db.execute("SELECT * FROM delivery_receipts WHERE episode_id=? ORDER BY id", (identifier,))]
                for receipt in receipts:
                    receipt["content"] = self.content(receipt.pop("payload"))
                feedback = [dict(r) for r in db.execute("SELECT * FROM feedback_events WHERE episode_id=? ORDER BY id LIMIT 1000", (identifier,))]
                return {"record": record, "receipts": receipts, "feedback": feedback}
            links = [dict(r) for r in db.execute("SELECT * FROM evidence_links WHERE evidence_id=? ORDER BY depth", (identifier,))]
            for link in links:
                link["context"] = json.loads(link["context"])
            return {"record": record, "links": links}
        return await self.service.store.inspect(detail)

    @staticmethod
    def content(payload):
        content = Content.load(payload)
        return {"text": content.plain, "categories": sorted(content.categories), "segments": len(content.segments)}
