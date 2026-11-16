"""Regression checks preserving behavior from astrbot_plugin_repeater.

Run: python -m unittest discover -s tests -p test_refactor_parity.py -v
"""
import asyncio
import base64
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from core import RuntimeState

from tiffany_plugin_repeater.config import RepeaterConfig
from tiffany_plugin_repeater.dashboard import Dashboard
from tiffany_plugin_repeater.engine import Activity, Engine
from tiffany_plugin_repeater.messages import MediaCache, parse_content
from tiffany_plugin_repeater.models import Content, GroupSettings, Segment
from tiffany_plugin_repeater.service import RepeaterService
from tiffany_plugin_repeater.storage import Store


class HighestWeight:
    def choices(self, population, weights, k=1):
        return [max(zip(population, weights), key=lambda item: item[1])[0]]

    def choice(self, population):
        return population[0]


class RefactorParityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="repeater-audit-")
        self.directory = Path(self.temporary.name)
        self.config = RepeaterConfig(answer_threshold=3, answer_weights=(1,),
                                     cooldown_seconds=0, duplicate_window=1,
                                     proactive_interval_seconds=3600, intelligence_mode="off")
        self.store = Store(self.directory, self.config)
        await self.store.open()
        for key in ("g1", "g2", "g3"):
            await self.store.ensure(key, "account", key)
        self.engine = Engine(self.store, self.config, rng=HighestWeight())
        self.sequence = 0

    async def asyncTearDown(self):
        await self.store.close()
        self.temporary.cleanup()

    async def train(self, group, prompt, answer, count):
        for _ in range(count):
            await self.store.barrier(group)
            self.sequence += 1
            await self.store.learn(group, prompt, "human", str(self.sequence), 100 + self.sequence)
            self.sequence += 1
            await self.store.learn(group, answer, "human", str(self.sequence), 100 + self.sequence)

    async def mark_sent(self, group, content, now=1000):
        token = await self.store.reserve(group, content, now, self.config, force=True)
        self.assertIsNotNone(token)
        await self.store.finish(token, now, True)

    async def test_forget_recalculates_the_remaining_relation_count(self):
        prompt = Content.text("天气情况如何")
        common, rare = Content.text("出门记得带伞呀"), Content.text("出门记得带伞哦")
        self.assertEqual(common.words, rare.words)
        await self.train("g1", prompt, rare, 1)
        rare_updated = (await self.store.candidates("account", prompt.words))[0]["updated"]
        await self.train("g1", prompt, common, 9)
        await self.mark_sent("g1", common)
        await self.store.forget("g1")
        rows = await self.store.candidates("account", prompt.words)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["count"], 1)
        self.assertEqual(rows[0]["relation_count"], 1,
                         "forget removed 9 observations, but the parent still counts 10")
        self.assertEqual(rows[0]["updated"], rare_updated)

    async def test_reopen_repairs_old_relation_totals_and_removes_empty_relations(self):
        prompt, answer = Content.text("天气情况如何"), Content.text("出门记得带伞")
        await self.train("g1", prompt, answer, 2)
        expected_updated = (await self.store.candidates("account", prompt.words))[0]["updated"]

        def corrupt(db):
            with db:
                db.execute("UPDATE relations SET count=99,updated=999 WHERE group_key='g1'")
                db.execute("INSERT INTO relations VALUES('g2','missing','missing',9,999)")

        await self.store.call(corrupt)
        await self.store.close()
        await self.store.open()
        rows = await self.store.candidates("account", prompt.words)
        self.assertEqual(rows[0]["relation_count"], 2)
        self.assertEqual(rows[0]["updated"], expected_updated)
        self.assertFalse(await self.store.candidates("account", "missing"))
        self.assertEqual(await self.store.call(lambda db: db.execute(
            "SELECT COUNT(*) FROM relations WHERE group_key='g2'").fetchone()[0]), 0)

    async def test_shared_variant_weights_include_every_source_group(self):
        mapping = {"触发正文": "prompt", "回答甲": "answer", "回答乙": "answer"}
        with patch("tiffany_plugin_repeater.models.keywords", side_effect=mapping.__getitem__):
            prompt, common, rare = map(Content.text, ("触发正文", "回答甲", "回答乙"))
            await self.train("g1", prompt, common, 1)
            await self.train("g1", prompt, rare, 2)
            await self.train("g2", prompt, common, 100)
            event = SimpleNamespace(group_key="g3", namespace="account")
            reply = await self.engine._answer(event, prompt)
            self.assertEqual(reply, common,
                             "variant learned 101 times loses to variant learned twice because remote counts are dropped")
            self.engine.config = replace(self.config, cross_group_threshold=3)
            self.assertIsNone(await self.engine._answer(event, prompt))
            local = SimpleNamespace(group_key="g1", namespace="account")
            self.assertEqual(await self.engine._answer(local, prompt), common)

    async def test_remote_media_cannot_raise_the_local_learning_count(self):
        prompt = Content.text("天气情况如何")
        answer = Content((Segment("image", "answer.gif", True),))
        await self.train("g1", prompt, answer, 1)
        await self.train("g2", prompt, answer, 5)
        self.assertIsNone(await self.engine._answer(
            SimpleNamespace(group_key="g1", namespace="account"), prompt))
        self.assertIsNone(await self.engine._answer(
            SimpleNamespace(group_key="g3", namespace="account"), prompt))

    async def test_topic_bonus_is_assigned_once_per_answer_keyword_bucket(self):
        mapping = {"触发正文": "prompt", "主题词": "alpha beta"}
        mapping.update({f"甲的说法{i}": "alpha" for i in range(5)})
        mapping["乙的说法"] = "alpha beta"

        class CaptureWeights(HighestWeight):
            recorded = None

            def choices(self, population, weights, k=1):
                population, weights = list(population), list(weights)
                if population and isinstance(population[0], tuple):
                    self.recorded = {contents[0].words: weight
                                     for (contents, _), weight in zip(population, weights)}
                return super().choices(population, weights, k)

        with patch("tiffany_plugin_repeater.models.keywords", side_effect=mapping.__getitem__):
            prompt = Content.text("触发正文")
            for index in range(5):
                await self.train("g1", prompt, Content.text(f"甲的说法{index}"), 1)
            await self.train("g1", prompt, Content.text("乙的说法"), 1)
            await self.store.learn("g1", Content.text("主题词"), "human", "topic", 999)
            self.engine.config = replace(self.config, answer_threshold=1, topics_size=2)
            capture = CaptureWeights()
            self.engine.rng = capture
            await self.engine._answer(SimpleNamespace(group_key="g1", namespace="account"), prompt)
            self.assertEqual(capture.recorded, {"alpha": 10005, "alpha beta": 20001})

    async def test_cooling_group_does_not_hide_other_proactive_groups(self):
        config = replace(self.config, cooldown_seconds=1000,
                         proactive_delay_seconds=0, proactive_activity_factor=0)
        service = RepeaterService(self.directory / "service", config, "review")
        await service.setup(SimpleNamespace(state=RuntimeState.RUNNING))
        service.active = True
        try:
            for key in ("first", "second", "third"):
                await service.store.ensure(key, "account", key)
                await service.store.learn(key, Content.text("可以主动发送的内容"), "human", key, 1001)
                service.engine.activity[key] = Activity(count=10, first=900, last=1001, spoke=1000)
            token = await service.store.reserve("first", Content.text("上次已发送"), 1000, config)
            await service.store.finish(token, 1000, True)
            service.engine.proactive = AsyncMock(wraps=service.engine.proactive)

            async def deliver(key, content, ctx):
                service.sender.delivered[key] = service.sender.delivered.get(key, 0) + 1

            service.sender.send = AsyncMock(side_effect=deliver)
            with (
                patch("tiffany_plugin_repeater.service.time.time", return_value=1002),
                patch("tiffany_plugin_repeater.service.asyncio.sleep",
                      new=AsyncMock(side_effect=[None, asyncio.CancelledError()])),
                self.assertRaises(asyncio.CancelledError),
            ):
                await service._maintenance()
            checked = [call.args[0] for call in service.engine.proactive.await_args_list]
            self.assertEqual(checked, ["first", "second"],
                             "the first group's cooldown rejects sending, but the loop breaks anyway")
            service.sender.send.assert_awaited_once()
            self.assertEqual(service.sender.send.await_args.args[0], "second")
            self.assertEqual(len(await service.store.recent("second", sent=True)), 1)
        finally:
            service.active = False
            await service.teardown()

    async def test_bracketed_expression_annotation_is_removed(self):
        cache = MediaCache(self.directory / "media", self.config)
        await cache.open()
        try:
            source = "base64://" + base64.b64encode(b"GIF89a" + bytes(20)).decode()
            for annotation in ("【动画表情】", "【商城表情】", "【表情：开心】", "[表情 : [? ]]", "[表情]", "【表情】"):
                with self.subTest(annotation=annotation):
                    event = SimpleNamespace(plain=annotation, segments=[
                        {"type": "text", "data": {"text": annotation}},
                        {"type": "image", "data": {"file": source}}])
                    content = await parse_content(event, cache, self.config, GroupSettings(text_enabled=False))
                    self.assertIsNotNone(content)
                    self.assertEqual(content.plain, "")
                    self.assertEqual(content.categories, {"expression"})
            event.plain = "真实配文【动画表情】"
            event.segments[0]["data"]["text"] = event.plain
            content = await parse_content(event, cache, self.config, GroupSettings())
            self.assertEqual(content.plain, "真实配文")
        finally:
            await cache.close()

    async def test_attachment_description_does_not_enable_a_normal_photo(self):
        cache = MediaCache(self.directory / "media", self.config)
        await cache.open()
        try:
            source = "base64://" + base64.b64encode(b"\x89PNG\r\n\x1a\n" + bytes(20)).decode()
            annotation = '<attachmentType="image/png",attachmentIndex=0,description="photo">'
            event = SimpleNamespace(plain=annotation, segments=[
                {"type": "text", "data": {"text": annotation}},
                {"type": "image", "data": {"file": source}}])
            self.assertIsNone(await parse_content(event, cache, self.config, GroupSettings()))
            content = await parse_content(event, cache, self.config, GroupSettings(media_enabled=True))
            self.assertEqual(content.categories, {"media"})
            self.assertEqual(content.plain, "")
        finally:
            await cache.close()

    async def test_raw_sticker_type_metadata_is_retained(self):
        cache = MediaCache(self.directory / "media", self.config)
        await cache.open()
        try:
            source = "base64://" + base64.b64encode(b"\x89PNG\r\n\x1a\n" + bytes(20)).decode()
            for metadata in ({"type": "emoji"}, {"_type": "STICKER"}, {"summary": "[商城表情]"},
                             {"is_sticker": True}, {"emoji_id": "123"}, {"emoji_package_id": "123"},
                             {"sub_type": "1"}, {"sub_type": "bad", "subType": 1}):
                with self.subTest(metadata=metadata):
                    event = SimpleNamespace(plain="", segments=[
                        {"type": "image", "data": {"file": source, **metadata}}])
                    content = await parse_content(event, cache, self.config, GroupSettings())
                    self.assertIsNotNone(content)
                    self.assertEqual(content.categories, {"expression"})
                    self.assertIsNone(await parse_content(event, cache, self.config,
                        GroupSettings(expression_enabled=False, media_enabled=True)))
            for metadata in ({"sub_type": "bad"}, {"sub_type": -1}, {"is_sticker": "false"}):
                with self.subTest(metadata=metadata):
                    event.segments[0]["data"] = {"file": source, **metadata}
                    self.assertIsNone(await parse_content(event, cache, self.config, GroupSettings()))
                    content = await parse_content(event, cache, self.config, GroupSettings(media_enabled=True))
                    self.assertEqual(content.categories, {"media"})
        finally:
            await cache.close()

    async def test_sent_dashboard_honors_the_media_filter(self):
        await self.mark_sent("g1", Content.text("普通文本"))
        await self.mark_sent("g1", Content((Segment("face", 42),)), now=1001)
        await self.mark_sent("g2", Content((Segment("text", "配文"), Segment("image", "example.gif", True))), now=1002)
        dashboard = Dashboard(SimpleNamespace(store=self.store))
        data = await dashboard.query("sent", {"media": "text"})
        self.assertEqual(data["total"], 1,
                         "the text filter returns both the text and the face send")
        data = await dashboard.query("sent", {"media": "media", "limit": "1"})
        self.assertEqual((data["total"], data["pages"], len(data["rows"])), (2, 2, 1))
        self.assertEqual(data["rows"][0]["group_key"], "g2")
        data = await dashboard.query("sent", {"media": "media", "group": "g1"})
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["rows"][0]["content"]["categories"], ["expression"])


if __name__ == "__main__":
    unittest.main()
