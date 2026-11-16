import asyncio
import base64
import sqlite3
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

import aiohttp
from adapters.onebot_fields import register_onebot_fields
from adapters.qqofficial_fields import register_qqofficial_fields
from core import APP_PATHS, Bot, Envelope
from shared.paths import RuntimePaths

from tiffany_plugin_repeater import SERVICE, RepeaterConfig, install
from tiffany_plugin_repeater.dashboard import Dashboard
from tiffany_plugin_repeater.messages import Incoming
from tiffany_plugin_repeater.models import Content, Segment
from tiffany_plugin_repeater.storage import Store


class FakeOneBot:
    running = True

    def __init__(self):
        self.calls = []
        self.sequence = 100
        self.fail = False

    async def call(self, action, params):
        if self.fail:
            raise RuntimeError("simulated platform failure")
        self.calls.append((action, params))
        self.sequence += 1
        return SimpleNamespace(data={"message_id": self.sequence})

    async def reply(self, ctx, text):
        return await self.call("send_group_msg", {"group_id": ctx.raw["group_id"], "message": text})


class FakeQQ:
    app_id = "test-app"

    def __init__(self):
        self.sent = []
        self.deleted = []
        self.uploaded = []
        self.fail_after = None

    async def reply(self, ctx, text):
        return await self.reply_message(ctx, json={"msg_type": 0, "content": text})

    async def reply_message(self, ctx, *, json):
        return await self.send_message("group", ctx.raw["d"]["group_openid"], json=json | {"test_passive": True})

    async def send_message(self, scene, target, *, json):
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise RuntimeError("simulated partial send")
        self.sent.append((scene, target, json))
        return {"id": "qq-message-" + str(len(self.sent))}

    async def upload_file(self, scene, target, path, *, file_type):
        self.uploaded.append((scene, target, Path(path), file_type))
        return {"file_info": "uploaded-file"}

    async def delete_message(self, scene, target, message_id):
        self.deleted.append(message_id)


class ConfigurationTests(unittest.TestCase):
    def test_generated_configuration_round_trips_startup_values(self):
        config = RepeaterConfig(enabled_default=False, ignored_user_ids=(12345, "另一个机器人🐢"),
                                delay_min_seconds=7.5, delay_max_seconds=12,
                                mute_duration_seconds=90,
                                dashboard_token='token with "quotes", \\slashes\n\x7f')
        with tempfile.TemporaryDirectory(prefix="repeater-config-") as directory:
            path = Path(directory) / "custom-home" / "config" / "repeater.toml"
            self.assertTrue(config.write_if_missing(path))
            self.assertEqual(RepeaterConfig.load(path), config)
            text = path.read_text("utf-8")
            self.assertIn("不学习、不触发接话", text)
            self.assertIn("随机等待", text)

    def test_existing_configuration_is_never_replaced_or_completed(self):
        with tempfile.TemporaryDirectory(prefix="repeater-config-") as directory:
            path = Path(directory) / "repeater.toml"
            for existing in (b"", b"# Keep my comments\r\ndelay_min_seconds = 1\r\n",
                             b"invalid TOML = ["):
                with self.subTest(existing=existing):
                    path.write_bytes(existing)
                    self.assertFalse(RepeaterConfig().write_if_missing(path))
                    self.assertEqual(path.read_bytes(), existing)

    def test_validated_options_and_presets(self):
        for options in ({"burst_limit": 0}, {"delay_min_seconds": 9, "delay_max_seconds": 2},
                        {"answer_weights": [0]}, {"enabled": "yes"}, {"dashboard_enabled": True},
                        {"admin_ids": "123"}, {"max_media_segments": 1.5}, {"cooldown_seconds": float("nan")},
                        {"mute_duration_seconds": 0}, {"mute_duration_seconds": -1},
                        {"unknown": 1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                RepeaterConfig.from_mapping(options)
        config = RepeaterConfig(repeat_threshold=5, ignored_user_ids=("opaque-openid",))
        self.assertEqual(config.effective("沉默的").repeat_threshold, 5)
        self.assertEqual(config.effective("话痨的").repeat_threshold, 2)
        self.assertEqual(config.effective("活跃的", True).answer_weights, (1,))

    def test_content_roundtrip_and_call_filter(self):
        content = Content((Segment("text", "配文"), Segment("image", "x.gif"), Segment("face", 42)))
        self.assertEqual(Content.load(content.dump()), content)
        self.assertEqual(content.categories, {"text", "expression"})
        self.assertIsNone(Content.text("龟龟晚安").answer(addressed=True))
        self.assertEqual(Content.text("龟龟，今天要记得吃饭呀").answer(addressed=True).plain, "今天要记得吃饭呀")


class NativeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="tiffany-repeater-test-")
        self.directory = Path(self.temporary.name)
        self.config = RepeaterConfig(cooldown_seconds=0, delay_min_seconds=0, delay_max_seconds=0,
                                     answer_threshold=1, answer_weights=(1,), burst_limit=100,
                                     admin_ids=("7", "opaque-openid"), proactive_interval_seconds=3600)
        self.bot = Bot()
        self.bot.service(APP_PATHS, RuntimePaths(self.directory))
        register_onebot_fields(self.bot, "napcat")
        register_qqofficial_fields(self.bot, "qq", lambda: "qq-bot")
        self.scope = self.bot.scope("plugins.repeater")
        install(self.scope, self.config)
        self.service = self.bot.services.get(SERVICE)
        self.client = FakeOneBot()
        self.qq = FakeQQ()
        self.counter = 0
        await self.bot.start()

    async def asyncTearDown(self):
        report = await self.bot.stop("abort")
        self.assertTrue(report.successful, report.failures)
        self.temporary.cleanup()

    async def message(self, text=None, *, segments=None, group=1, author=2, event_id=None, role="member", client=None, adapter="onebot:test", account=9):
        self.counter += 1
        raw = {"post_type": "message", "message_type": "group", "self_id": account, "user_id": author,
               "group_id": group, "message_id": event_id or self.counter,
               "sender": {"role": role}, "message": segments if segments is not None else [{"type": "text", "data": {"text": text}}]}
        return await self.bot.emit(Envelope("napcat", raw, client=client or self.client, kind="message", adapter_id=adapter, session_id=f"group:{group}"))

    async def qqmessage(self, text, *, group="qq-group", attachments=None):
        self.counter += 1
        raw = {"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": str(self.counter), "group_openid": group,
                "author": {"member_openid": "opaque-openid"}, "content": text, "attachments": attachments or []}}
        return await self.bot.emit(Envelope("qq", raw, client=self.qq, kind="message", adapter_id="qq:test-app", session_id=group))

    async def settle(self):
        if self.service.pending:
            await asyncio.gather(*tuple(self.service.pending.values()), return_exceptions=True)

    async def current_event(self, group=1):
        ctx = await self.message("", group=group)
        return Incoming.read(ctx)

    async def test_commands_to_other_accounts_do_not_execute_or_enter_corpus(self):
        commands = ("/repeater off", "/repeater on", "/repeater frequency 话痨的",
                    "/repeater intelligence off", "/repeater media on", "/repeater text off",
                    "/repeater expression off", "/repeater status", "/repeater forget",
                    "/repeater forget_keyword", "/repeater clear confirm", "龟龟喝酒",
                    "龟龟醒酒", "龟龟顶人", "龟龟缩头", "龟龟闭嘴", "龟龟说话")
        event = await self.current_event()
        original = await self.service.store.settings(event.group_key)
        for command in commands:
            with self.subTest(command=command):
                ctx = await self.message(author=7, segments=[{"type": "at", "data": {"qq": "10"}},
                                                           {"type": "text", "data": {"text": command}}])
                self.assertFalse(Incoming.read(ctx).command_addressed)
        self.assertFalse(self.client.calls)
        self.assertFalse(self.service.pending)
        self.assertFalse(self.service.sober_tasks)
        self.assertFalse(self.service.muted_until)
        self.assertEqual(await self.service.store.settings(event.group_key), original)
        self.assertFalse(await self.service.store.recent(event.group_key))

    async def test_targeted_admin_command_is_isolated_by_account_in_same_group(self):
        other = FakeOneBot()
        segments = [{"type": "at", "data": {"qq": "9"}},
                    {"type": "text", "data": {"text": "/repeater frequency 活跃的"}}]
        ours = Incoming.read(await self.message(segments=segments, author=7, event_id=700))
        theirs = Incoming.read(await self.message(segments=segments, author=7, event_id=700, account=10, client=other))
        self.assertEqual((await self.service.store.settings(ours.group_key)).preset, "活跃的")
        self.assertEqual((await self.service.store.settings(theirs.group_key)).preset, "沉默的")
        self.assertEqual(len(self.client.calls), 1)
        self.assertFalse(other.calls)

    async def test_all_or_malformed_mentions_do_not_broadcast_commands(self):
        for data in ({"qq": "all"}, {}, None):
            with self.subTest(data=data):
                await self.message(author=7, segments=[{"type": "at", "data": data},
                                                      {"type": "text", "data": {"text": "/repeater off"}}])
        self.assertFalse(self.client.calls)
        event = await self.current_event()
        self.assertTrue((await self.service.store.settings(event.group_key)).enabled)
        # Mentioning this account alongside another member is still addressed to us.
        await self.message(author=7, segments=[{"type": "at", "data": {"qq": "123"}},
                                              {"type": "at", "data": {"qq": 9}},
                                              {"type": "text", "data": {"text": "/ repeater off"}}])
        self.assertFalse((await self.service.store.settings(event.group_key)).enabled)

    async def test_qq_mentions_target_configuration_and_mute_commands(self):
        for command in ("/repeater off", "龟龟喝酒", "龟龟闭嘴"):
            await self.qqmessage("<@!another-bot> " + command)
        await self.qqmessage("<@&qq-bot> /repeater off")
        self.assertFalse(self.qq.sent)
        event = Incoming.read(await self.qqmessage("<@!qq-bot> 龟龟闭嘴"))
        self.assertGreater(self.service.mute_remaining(event.group_key), 1700)
        await self.qqmessage("<@another-bot> 龟龟说话")
        self.assertGreater(self.service.mute_remaining(event.group_key), 1700)
        await self.qqmessage("<@qq-bot> 龟龟说话")
        self.assertEqual(self.service.mute_remaining(event.group_key), 0)
        await self.qqmessage("<@qq-bot> /repeater off")
        self.assertFalse((await self.service.store.settings(event.group_key)).enabled)
        self.assertEqual(len(self.qq.sent), 3)

    async def test_mute_cancels_pending_replies_but_keeps_learning(self):
        self.service.engine.config = replace(self.config, delay_min_seconds=60, delay_max_seconds=60)
        for _ in range(3):
            ctx = await self.message("静音前等待发送的复读")
        event = Incoming.read(ctx)
        pending = self.service.pending[event.group_key]
        await self.message(segments=[{"type": "at", "data": {"qq": "9"}},
                                     {"type": "text", "data": {"text": "龟龟闭嘴"}}])
        await asyncio.gather(pending, return_exceptions=True)
        self.assertTrue(pending.cancelled())
        self.assertFalse(self.service.pending)
        self.assertGreater(self.service.mute_remaining(event.group_key), 1700)
        self.assertLessEqual(self.service.mute_remaining(event.group_key), 1800)
        self.assertEqual(self.client.calls[-1][1]["message"], "哼，闭嘴就闭嘴，在你说‘龟龟说话‘前我不会原谅你的")
        for _ in range(3):
            await self.message("静音后仍然学习的复读")
        self.assertEqual(len(await self.service.store.recent(event.group_key)), 6)
        self.assertFalse(self.service.pending)
        self.assertEqual(len(self.client.calls), 1)
        await self.message("/repeater status", author=7)
        self.assertIn("静音剩余：", self.client.calls[-1][1]["message"])

    async def test_mute_is_scoped_to_one_account_and_one_group(self):
        other = FakeOneBot()
        target = [{"type": "at", "data": {"qq": "9"}},
                  {"type": "text", "data": {"text": "龟龟闭嘴"}}]
        ours = Incoming.read(await self.message(segments=target))
        theirs = Incoming.read(await self.message(segments=target, account=10, client=other))
        elsewhere = Incoming.read(await self.message("", group=2))
        self.assertGreater(self.service.mute_remaining(ours.group_key), 0)
        self.assertEqual(self.service.mute_remaining(theirs.group_key), 0)
        self.assertEqual(self.service.mute_remaining(elsewhere.group_key), 0)
        self.assertFalse(other.calls)
        self.assertTrue(await self.service._send(theirs.group_key, Content.text("另一个账号仍能说话")))
        self.assertTrue(await self.service._send(elsewhere.group_key, Content.text("另一个群仍能说话")))
        # An unmute addressed to the other account must not release this account.
        await self.message(segments=[{"type": "at", "data": {"qq": "10"}},
                                     {"type": "text", "data": {"text": "龟龟说话"}}])
        self.assertGreater(self.service.mute_remaining(ours.group_key), 0)

    async def test_mute_suppresses_forced_proactive_and_sober_speech(self):
        event = Incoming.read(await self.message("龟龟闭嘴"))
        self.client.calls.clear()
        for command in ("龟龟顶人", "龟龟缩头", "龟龟喝酒", "龟龟醒酒"):
            await self.message(command)
        self.assertFalse(await self.service._send(event.group_key, Content.text("不能主动说话")))
        self.assertFalse(await self.service._send(event.group_key, Content.text("不能强制说话"), event.ctx, force=True))
        deadline = time.time() - 1
        self.service.engine.drunk_until[event.group_key] = deadline
        await self.service._auto_sober(event.group_key, deadline)
        self.assertFalse(self.client.calls)
        self.assertNotIn(event.group_key, self.service.engine.drunk_until)

    async def test_mute_refresh_expiry_and_manual_resume_preserve_group_switch(self):
        event = Incoming.read(await self.message("龟龟闭嘴"))
        self.service.muted_until[event.group_key] = time.monotonic() + 10
        await self.message("/ 龟龟闭嘴")
        self.assertGreater(self.service.mute_remaining(event.group_key), 1700)
        self.service.muted_until[event.group_key] = time.monotonic() - 1
        before = len(self.client.calls)
        await self.message("龟龟顶人")
        self.assertEqual(len(self.client.calls), before + 1)
        self.assertNotIn(event.group_key, self.service.muted_until)
        await self.message("龟龟闭嘴")
        await self.message("龟龟说话")
        self.assertEqual(self.service.mute_remaining(event.group_key), 0)
        await self.message("/repeater off", author=7)
        await self.message("龟龟闭嘴")
        await self.message("龟龟说话")
        self.assertFalse((await self.service.store.settings(event.group_key)).enabled)
        self.assertIn("仍处于关闭状态", self.client.calls[-1][1]["message"])

    async def test_maintenance_skips_muted_group_and_checks_other_groups(self):
        muted = Incoming.read(await self.message("龟龟闭嘴"))
        other = Incoming.read(await self.message("另一个群的学习消息", group=2))
        # Include both groups regardless of their actual proactive eligibility.
        self.service.engine.activity[muted.group_key] = object()
        proactive = AsyncMock(return_value=None)
        self.service.maintenance.cancel()
        await asyncio.gather(self.service.maintenance, return_exceptions=True)
        with (
            patch.object(self.service.engine, "proactive", proactive),
            patch("tiffany_plugin_repeater.service.asyncio.sleep",
                  new=AsyncMock(side_effect=[None, asyncio.CancelledError()])),
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service._maintenance()
        proactive.assert_awaited_once_with(other.group_key, ANY)

    async def test_native_hook_three_repeats_and_receipt_recall(self):
        for _ in range(3):
            await self.message("这是一句复读测试")
        await self.settle()
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.client.calls[0][1]["message"][0]["data"]["text"], "这是一句复读测试")
        await self.message("龟龟缩头")
        self.assertEqual(self.client.calls[1][0], "delete_msg")
        self.assertEqual(self.client.calls[2][1]["message"], "很抱歉我的头可能会不小心顶到人，我会尽快缩回去")

    async def test_identical_texts_with_new_ids_are_not_duplicate_events(self):
        await self.message("重复的消息", event_id=500)
        await self.message("重复的消息", event_id=500)
        await self.message("重复的消息", event_id=501)
        event = await self.current_event()
        rows = await self.service.store.recent(event.group_key)
        self.assertEqual(len(rows), 2)
        self.assertFalse(self.client.calls)

    async def test_mentions_removed_and_invalid_message_breaks_relation(self):
        await self.message(segments=[{"type": "at", "data": {"qq": "123456"}}, {"type": "text", "data": {"text": "问候正文"}}])
        event = await self.current_event()
        records = await self.service.store.recent(event.group_key)
        self.assertEqual(Content.load(records[0]["payload"]).plain, "问候正文")
        await self.message("后面的回答")
        candidates = await self.service.store.candidates(event.namespace, records[0]["words"])
        self.assertFalse(candidates)

    async def test_context_learning_uses_existing_corpus_before_new_message(self):
        for _ in range(3):
            await self.message("今天天气不错")
            await self.message("出门记得带伞")
            await self.settle()
        self.client.calls.clear()
        await self.message("今天天气不错")
        await self.settle()
        self.assertEqual(self.client.calls[-1][1]["message"][0]["data"]["text"], "出门记得带伞")

    async def test_permissions_frequency_switch_persistence_and_clear(self):
        await self.message("/repeater off")
        self.assertIn("只有管理员", self.client.calls[-1][1]["message"])
        await self.message("/repeater frequency 话痨的", author=7)
        await self.message("/repeater media on", role="admin")
        await self.message("有效的群聊语料")
        event = await self.current_event()
        await self.message("/repeater clear confirm", author=7)
        settings = await self.service.store.settings(event.group_key)
        self.assertEqual(settings.preset, "话痨的")
        self.assertTrue(settings.media_enabled)
        self.assertFalse(await self.service.store.recent(event.group_key))
        await self.message("/repeater off", author=7)
        await self.message("这句不能被收录")
        self.assertFalse(await self.service.store.recent(event.group_key))

    async def test_delay_does_not_block_hook_and_off_cancels_timer(self):
        self.service.engine.config = replace(self.config, delay_min_seconds=20, delay_max_seconds=20)
        for _ in range(3):
            await asyncio.wait_for(self.message("延迟复读内容"), .8)
        self.assertTrue(self.service.pending)
        await asyncio.wait_for(self.message("/repeater off", author=7), .8)
        self.assertFalse(self.service.pending)
        await asyncio.sleep(0)
        event = await self.current_event()
        self.assertFalse(await self.service.store.recent(event.group_key, sent=True))

    async def test_category_is_rechecked_at_send_time(self):
        event = await self.current_event()
        await self.service.store.configure(event.group_key, "text_enabled", False)
        self.assertFalse(await self.service._send(event.group_key, Content.text("不应该发出的消息")))
        self.assertFalse(self.client.calls)

    async def test_send_failure_does_not_pollute_history(self):
        event = await self.current_event()
        self.client.fail = True
        with self.assertLogs("tiffany_plugin_repeater.service", level="ERROR"):
            self.assertFalse(await self.service._send(event.group_key, Content.text("发送失败的内容")))
        self.assertFalse(await self.service.store.recent(event.group_key, sent=True))
        self.client.fail = False
        self.assertTrue(await self.service._send(event.group_key, Content.text("发送失败的内容")))

    async def test_reservation_is_single_use_and_concurrent_sends_serialize(self):
        event = await self.current_event()
        content = Content.text("并发发送的内容")
        results = await asyncio.gather(*[self.service.store.reserve(event.group_key, content, time.time(), self.config) for _ in range(8)])
        self.assertEqual(sum(token is not None for token in results), 1)
        await self.service.store.finish(next(t for t in results if t), time.time(), True)
        self.assertEqual(len(await self.service.store.recent(event.group_key, sent=True)), 1)

    async def test_drink_alias_refresh_and_sober_do_not_enter_corpus(self):
        await self.message("/ 龟龟喝酒")
        event = await self.current_event()
        self.assertGreater(self.service.engine.remaining(event.group_key), 0)
        await self.message("龟龟继续喝")
        self.assertEqual(len(self.service.sober_tasks), 1)
        await self.message("龟龟醒一醒")
        self.assertEqual(self.service.engine.remaining(event.group_key), 0)
        self.assertFalse(await self.service.store.recent(event.group_key))
        self.assertFalse(self.service.sender.receipts)

    async def test_automatic_sober_is_sent_without_becoming_recall_candidate(self):
        event = await self.current_event()
        deadline = time.time() + .02
        self.service.engine.drunk_until[event.group_key] = deadline
        await self.service._auto_sober(event.group_key, deadline)
        self.assertEqual(self.client.calls[-1][1]["message"][0]["data"]["text"], "唔~~刚刚发生什么了")
        self.assertFalse(self.service.sender.receipts)

    async def test_native_qq_openid_commands_and_recall(self):
        ctx = await self.qqmessage("<@!qq-bot> /repeater frequency 活跃的")
        event = Incoming.read(ctx)
        self.assertEqual((await self.service.store.settings(event.group_key)).preset, "活跃的")
        await self.qqmessage("龟龟顶人")
        self.assertTrue(self.service.sender.receipts[event.group_key])
        await self.qqmessage("龟龟缩头")
        self.assertEqual(len(self.qq.deleted), 1)

    async def test_account_namespaces_and_reconnection_refresh(self):
        ctx1 = await self.message("第一账号的消息")
        ctx2 = await self.message("另一个账号的消息", adapter="onebot:other")
        self.assertNotEqual(Incoming.read(ctx1).group_key, Incoming.read(ctx2).group_key)
        replacement = FakeOneBot()
        self.client.running = False
        await self.message("重连后其他群的消息", group=2, client=replacement)
        event = Incoming.read(ctx1)
        await self.service._send(event.group_key, Content.text("重连后发送"))
        self.assertTrue(replacement.calls)
        self.client.running = True

    async def test_gif_cache_classification_and_native_base64_send(self):
        data = b"GIF89a" + bytes(20)
        source = "base64://" + base64.b64encode(data).decode()
        segments = [{"type": "text", "data": {"text": "[表情]"}}, {"type": "image", "data": {"file": source}}]
        for _ in range(3):
            await self.message(segments=segments)
        await self.settle()
        self.assertEqual(len(self.client.calls), 1)
        message = self.client.calls[0][1]["message"]
        self.assertEqual(len(message), 1)
        self.assertEqual(message[0]["data"]["file"], source)

    async def test_malformed_media_and_links_do_not_enter_corpus(self):
        for segments in ([{"type": "face", "data": {"id": "bad"}}], [{"type": "image", "data": "bad"}], [{"type": "record", "data": {}}]):
            await self.message(segments=segments)
        await self.message("https://example.com")
        event = await self.current_event()
        self.assertFalse(await self.service.store.recent(event.group_key))

    async def test_file_uri_is_portable_and_media_size_is_bounded(self):
        path = self.directory / "local image.gif"
        path.write_bytes(b"GIF89a" + bytes(20))
        name = await self.service.cache.cache(path.as_uri())
        self.assertEqual(self.service.cache.path(name).read_bytes(), path.read_bytes())
        from tiffany_plugin_repeater.messages import MediaError
        self.service.cache.config = replace(self.config, max_image_bytes=10)
        with self.assertRaises(MediaError):
            await self.service.cache.cache(path.as_uri())
        with self.assertRaises(MediaError):
            self.service.cache.path("../private.png")

    async def test_qq_partial_send_commits_and_retains_actual_receipt(self):
        ctx = await self.qqmessage("合法正文")
        event = Incoming.read(ctx)
        name = await self.service.cache.cache("base64://" + base64.b64encode(b"GIF89a" + bytes(20)).decode())
        content = Content((Segment("image", name, True), Segment("image", name, True)))
        self.qq.fail_after = 1
        with self.assertLogs("tiffany_plugin_repeater.service", level="ERROR"):
            self.assertTrue(await self.service._send(event.group_key, content, ctx))
        self.assertEqual(len(await self.service.store.recent(event.group_key, sent=True)), 1)
        self.assertEqual(len(self.service.sender.receipts[event.group_key]), 1)
        await self.service.sender.recall(event.group_key)
        self.assertEqual(self.qq.deleted, ["qq-message-1"])

    async def test_proactive_requires_runtime_activity_and_only_sends_once(self):
        self.service.engine.config = replace(self.config, proactive_delay_seconds=0, proactive_activity_factor=0)
        for index in range(10):
            await self.message(f"第{index}条测试消息")
        event = await self.current_event()
        self.assertIsNone(await self.service.engine.proactive(event.group_key, time.time() + 1))
        await self.service._send(event.group_key, Content.text("主动发言前的回复"))
        await self.message("最后一条群友消息")
        content = await self.service.engine.proactive(event.group_key, time.time() + 1)
        self.assertIsNotNone(content)
        await self.service._send(event.group_key, content)
        self.assertIsNone(await self.service.engine.proactive(event.group_key, time.time() + 1))

    async def test_forget_bans_content_and_keyword_without_resetting_switches(self):
        event = await self.current_event()
        content = Content.text("等待封禁的回复")
        await self.service._send(event.group_key, content)
        await self.message("/repeater forget_keyword", author=7)
        self.assertFalse(await self.service.engine.allowed(event.group_key, content))
        self.assertTrue((await self.service.store.settings(event.group_key)).enabled)

    async def test_dashboard_filters_auth_and_read_only_routes(self):
        await self.message("语料观察台测试")
        self.service.config = replace(self.config, dashboard_enabled=True, dashboard_token="test-token-long-enough", dashboard_port=0)
        dashboard = Dashboard(self.service)
        await dashboard.open()
        try:
            base = f"http://127.0.0.1:{dashboard.port}"
            async with aiohttp.ClientSession() as session:
                async with session.get(base + "/api/messages") as response:
                    self.assertEqual(response.status, 401)
                headers = {"Authorization": "Bearer test-token-long-enough"}
                async with session.get(base + "/api/messages", headers=headers, params={"text": "观察台", "media": "text"}) as response:
                    self.assertEqual(response.status, 200)
                    result = await response.json()
                    self.assertEqual(result["total"], 1)
                async with session.post(base + "/api/messages", headers=headers) as response:
                    self.assertEqual(response.status, 405)
                async with session.get(base + "/api/messages?limit=1000", headers=headers) as response:
                    self.assertEqual(response.status, 400)
                async with session.get(base + "/api/detail?table=messages&id=" + str(result["rows"][0]["id"]), headers=headers) as response:
                    self.assertEqual(len((await response.json())["sources"]), 1)
        finally:
            await dashboard.close()

    async def test_scope_unload_removes_hook_timers_and_closes_store(self):
        await self.message("龟龟喝酒")
        await self.scope.unload(mode="abort")
        self.assertFalse(self.bot.runtime.tasks.tasks_for(self.scope.owner))
        self.assertIsNone(self.service.store._executor)
        await self.message("卸载以后不会被学习")
        self.assertFalse(self.service.active)

    async def test_unknown_schema_retains_existing_database(self):
        path = self.directory / "unknown"
        path.mkdir()
        db = sqlite3.connect(path / "repeater.sqlite3")
        db.execute("CREATE TABLE valuable(value)")
        db.execute("INSERT INTO valuable VALUES('keep')")
        db.execute("PRAGMA user_version=99")
        db.commit()
        db.close()
        store = Store(path, self.config)
        with self.assertRaises(ValueError):
            await store.open()
        check = sqlite3.connect(store.path)
        self.assertEqual(check.execute("SELECT value FROM valuable").fetchone()[0], "keep")
        check.close()

    async def test_cross_group_text_threshold_and_media_locality(self):
        first = await self.current_event(1)
        second = await self.current_event(2)
        other = await self.current_event(3)
        prompt, answer = Content.text("天气情况如何"), Content.text("出门记得带伞")
        async def train(event, question, reply):
            await self.service.engine.learned(event, question, time.time())
            await self.service.engine.learned(event, reply, time.time())
        await train(first, prompt, answer)
        self.assertIsNone(await self.service.engine.choose(other, prompt))
        await train(second, prompt, answer)
        self.assertEqual(await self.service.engine.choose(other, prompt), answer)
        picture_prompt = Content((Segment("text", "带配文的表情"), Segment("image", "prompt.gif", True)))
        picture_answer = Content((Segment("image", "answer.gif", True),))
        await train(first, picture_prompt, picture_answer)
        await train(second, picture_prompt, picture_answer)
        self.assertIsNone(await self.service.engine.choose(other, picture_prompt))
        foreign = Incoming.read(await self.message("", adapter="another-account"))
        self.assertIsNone(await self.service.engine.choose(foreign, prompt))

    async def test_store_survives_reopen_and_releases_cancelled_operations(self):
        import threading
        event = await self.current_event()
        await self.service.store.configure(event.group_key, "preset", "活跃的")
        entered, release = threading.Event(), threading.Event()
        def slow(db):
            entered.set()
            release.wait(2)
            with db:
                db.execute("UPDATE groups SET text_enabled=0 WHERE key=?", (event.group_key,))
        operation = asyncio.create_task(self.service.store.call(slow))
        await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await operation
        self.assertTrue(self.service.store._pending)
        release.set()
        await self.service.store.close()
        await self.service.store.open()
        settings = await self.service.store.settings(event.group_key)
        self.assertEqual(settings.preset, "活跃的")
        self.assertFalse(settings.text_enabled)

    async def test_builtin_commands_permissions_and_learning_barriers(self):
        from hooks import install_extensions
        from shared.extensions import ExtensionsConfig
        await self.bot.stop()
        self.bot = Bot()
        self.bot.service(APP_PATHS, RuntimePaths(self.directory))
        register_onebot_fields(self.bot, "napcat")
        extensions = ExtensionsConfig.parse({"enabled": ["commands", "ping", "permissions", "notes"],
                       "permissions": {"admins": [{"adapter_id": "onebot:test", "scene": "group", "user_id": 7}]}})
        await install_extensions(self.bot, extensions)
        self.scope = self.bot.scope("plugins.repeater")
        install(self.scope, replace(self.config, admin_ids=(), allow_group_admin=False))
        self.service = self.bot.services.get(SERVICE)
        await self.bot.start()
        await self.message("指令之前的正文")
        await self.message("/note set saved")
        await self.message("指令之后的正文")
        await self.message("/ping")
        await self.message("/repeater off", author=7)
        self.assertIn("保存成功", [params["message"] for _, params in self.client.calls])
        self.assertIn("pong", [params["message"] for _, params in self.client.calls])
        event = await self.current_event()
        self.assertFalse((await self.service.store.settings(event.group_key)).enabled)
        rows = await self.service.store.recent(event.group_key)
        self.assertEqual(len(rows), 2)
        self.assertFalse(await self.service.store.candidates(event.namespace, Content.text("指令之前的正文").words))

    async def test_graceful_drain_commits_already_admitted_messages(self):
        import threading
        entered, release = threading.Event(), threading.Event()
        blocker = asyncio.create_task(self.service.store.call(lambda db: (entered.set(), release.wait(3))))
        await asyncio.to_thread(entered.wait, 1)
        first = asyncio.create_task(self.message("停止之前已接纳的第一条"))
        second = asyncio.create_task(self.message("停止之前已接纳的第二条"))
        try:
            for _ in range(100):
                if self.bot.runtime.scheduler.queued:
                    break
                await asyncio.sleep(.005)
            stop = asyncio.create_task(self.bot.stop("drain"))
            await asyncio.sleep(.01)
            release.set()
            await blocker
            await asyncio.gather(first, second)
            self.assertTrue((await stop).successful)
            db = sqlite3.connect(self.service.store.path)
            try:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 2)
            finally:
                db.close()
        finally:
            release.set()
