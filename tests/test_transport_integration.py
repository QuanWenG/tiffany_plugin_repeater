"""A real Tiffany Adapter and Client exchange frames with a local OneBot peer."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from adapters.OneBotWebSocketAdapter import OneBotWebSocketAdapter
from aiohttp import web
from core import APP_PATHS, Bot
from shared.paths import RuntimePaths
from websockets.asyncio.client import connect

from tiffany_plugin_repeater import SERVICE, RepeaterConfig, install


class NativeTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_onebot_targeted_mute_and_manual_resume(self):
        with tempfile.TemporaryDirectory(prefix="tiffany-repeater-mute-") as directory:
            bot = Bot()
            bot.service(APP_PATHS, RuntimePaths(Path(directory)))
            install(bot.scope("plugins.repeater"), RepeaterConfig(delay_min_seconds=0, delay_max_seconds=0))
            adapter = bot.install(OneBotWebSocketAdapter("127.0.0.1", 0, "napcat", call_timeout=2))
            try:
                await bot.start()
                port = adapter._server.sockets[0].getsockname()[1]
                async with connect(f"ws://127.0.0.1:{port}") as ws:
                    identifier = 0
                    async def incoming(text, mention=None):
                        nonlocal identifier
                        identifier += 1
                        segments = ([{"type": "at", "data": {"qq": str(mention)}}] if mention else [])
                        segments.append({"type": "text", "data": {"text": text}})
                        await ws.send(json.dumps({"post_type": "message", "message_type": "group", "self_id": 99,
                            "group_id": 1, "user_id": 2, "message_id": identifier, "message": segments}))
                    async def response(request):
                        await ws.send(json.dumps({"echo": request["echo"], "status": "ok", "retcode": 0,
                                                 "data": {"message_id": identifier + 1000}}))
                    # The first frame targets another account and must not produce a reply.
                    await incoming("龟龟喝酒", mention=100)
                    await incoming("龟龟闭嘴", mention=99)
                    request = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    self.assertEqual(request["params"]["message"], "哼，闭嘴就闭嘴，在你说‘龟龟说话‘前我不会原谅你的")
                    await response(request)
                    for _ in range(3):
                        await incoming("静音期间保持安静")
                    await incoming("龟龟顶人", mention=99)
                    await incoming("龟龟说话", mention=100)
                    await incoming("龟龟说话", mention=99)
                    request = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    self.assertEqual(request["params"]["message"], "看在你诚心诚意，我就原谅你吧")
                    await response(request)
                    for _ in range(3):
                        await incoming("恢复后的正常复读")
                    request = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    self.assertEqual(request["params"]["message"][0]["data"]["text"], "恢复后的正常复读")
                    await response(request)
                    await asyncio.wait_for(bot.runtime.scheduler.drain(2), 3)
                    service = bot.services.get(SERVICE)
                    self.assertFalse(service.muted_until)
                    key = next(iter(service.engine.activity))
                    self.assertEqual(len(await service.store.recent(key)), 6)
            finally:
                self.assertTrue((await bot.stop("abort")).successful)

    async def test_real_onebot_api_reply_receipt_and_recall(self):
        with tempfile.TemporaryDirectory(prefix="tiffany-repeater-ws-") as directory:
            bot = Bot()
            bot.service(APP_PATHS, RuntimePaths(Path(directory)))
            install(bot.scope("plugins.repeater"), RepeaterConfig(delay_min_seconds=0, delay_max_seconds=0, cooldown_seconds=0))
            adapter = bot.install(OneBotWebSocketAdapter("127.0.0.1", 0, "napcat", call_timeout=2))
            try:
                await bot.start()
                port = adapter._server.sockets[0].getsockname()[1]
                async with connect(f"ws://127.0.0.1:{port}") as ws:
                    async def incoming(index, text):
                        await ws.send(json.dumps({"post_type": "message", "message_type": "group", "self_id": 99,
                            "group_id": 1, "user_id": 2, "message_id": index, "message": text}))
                    async def response(request, message_id):
                        await ws.send(json.dumps({"echo": request["echo"], "status": "ok", "retcode": 0, "data": {"message_id": message_id}}))
                    for index in range(3):
                        await incoming(index + 1, "通过真实客户端复读")
                    request = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    self.assertEqual(request["action"], "send_group_msg")
                    self.assertEqual(request["params"]["message"][0]["data"]["text"], "通过真实客户端复读")
                    await response(request, 500)
                    # Exercise notice delivery through the real websocket adapter,
                    # including cancellation and an explicit mocking quote.
                    for actor in (3, 4):
                        await ws.send(json.dumps({"post_type": "notice", "notice_type": "group_msg_emoji_like",
                            "self_id": 99, "group_id": 1, "user_id": actor, "message_id": 500,
                            "likes": [{"emoji_id": "128", "count": 1}], "is_add": True}))
                    await ws.send(json.dumps({"post_type": "notice", "notice_type": "group_msg_emoji_like",
                        "self_id": 99, "group_id": 1, "user_id": 3, "message_id": 500,
                        "likes": [{"emoji_id": "128", "count": 0}], "is_add": False}))
                    await ws.send(json.dumps({"post_type": "message", "message_type": "group", "self_id": 99,
                        "group_id": 1, "user_id": 4, "message_id": 40,
                        "message": [{"type": "reply", "data": {"id": "500"}},
                                    {"type": "text", "data": {"text": "你可真是个人才"}}]}))
                    await incoming(4, "龟龟缩头")
                    request = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    self.assertEqual(request["action"], "delete_msg")
                    self.assertEqual(request["params"]["message_id"], 500)
                    await response(request, 501)
                    request = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    self.assertIn("缩回去", request["params"]["message"])
                    await response(request, 502)
                    await asyncio.wait_for(bot.runtime.scheduler.drain(2), 3)
                    store = bot.services.get(SERVICE).store
                    episode = await store.read(lambda db: dict(db.execute("SELECT * FROM reply_episodes LIMIT 1").fetchone()))
                    self.assertEqual((episode["participants"], episode["reward"]), (1, .5))
                    self.assertEqual(await store.read(lambda db: db.execute("SELECT COUNT(*) FROM learning_evidence").fetchone()[0]), 3)
                self.assertEqual(len(bot.services.get(SERVICE).sender.delivered), 1)
            finally:
                self.assertTrue((await bot.stop("abort")).successful)

    async def test_real_qq_client_passive_sequence_and_delete(self):
        from adapters.qqofficial_fields import register_qqofficial_fields
        from clients.QQOfficialClient import QQOfficialClient
        from core import Envelope, ServiceKey
        requests, deletes = [], []
        async def token(request):
            return web.json_response({"access_token": "local-test-token", "expires_in": 3600})
        async def send(request):
            self.assertEqual(request.headers["Authorization"], "QQBot local-test-token")
            requests.append(await request.json())
            return web.json_response({"id": "official-" + str(len(requests))})
        async def delete(request):
            deletes.append(request.match_info["message"])
            return web.Response(status=204)
        app = web.Application()
        app.router.add_post("/token", token)
        app.router.add_post("/v2/groups/{group}/messages", send)
        app.router.add_delete("/v2/groups/{group}/messages/{message}", delete)
        server = web.AppRunner(app)
        await server.setup()
        try:
            await web.TCPSite(server, "127.0.0.1", 0).start()
            base = "http://127.0.0.1:" + str(server.addresses[0][1])
            with tempfile.TemporaryDirectory(prefix="tiffany-repeater-qq-") as directory:
                client = QQOfficialClient("local-app", "local-secret", api_base=base, token_url=base + "/token")
                bot = Bot()
                bot.service(APP_PATHS, RuntimePaths(Path(directory)))
                bot.service(ServiceKey("test.qq.client"), client)
                register_qqofficial_fields(bot, "qq", lambda: "qq-bot")
                install(bot.scope("plugins.repeater"), RepeaterConfig(delay_min_seconds=0, delay_max_seconds=0))
                try:
                    await bot.start()
                    async def incoming(identifier, text):
                        raw = {"t": "GROUP_MESSAGE_CREATE", "d": {"id": str(identifier), "group_openid": "group-openid",
                            "author": {"member_openid": "member-openid"}, "content": text}}
                        return await bot.emit(Envelope("qq", raw, client=client, kind="message", adapter_id="qq:app", session_id="group"))
                    for index in range(3):
                        await incoming(index + 1, "通过原生 QQ Client 复读")
                    await asyncio.gather(*tuple(bot.services.get(SERVICE).pending.values()))
                    self.assertEqual(requests[0]["msg_id"], "3")
                    self.assertEqual(requests[0]["msg_seq"], 1)
                    await incoming(4, "龟龟缩头")
                    self.assertEqual(deletes, ["official-1"])
                    self.assertIn("缩回去", requests[1]["content"])
                    self.assertEqual(requests[1]["msg_id"], "4")
                finally:
                    self.assertTrue((await bot.stop("abort")).successful)
        finally:
            await server.cleanup()
