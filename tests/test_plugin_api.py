"""Exercise the host's API 1 discovery and installation, not a local shim."""
import asyncio
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from adapters.onebot_fields import register_onebot_fields
from application import TiffanyApplication
from core import APP_PATHS, Bot, Envelope, RuntimeState
from deployment.plugin_worker import check_registration
from hooks.plugins import install_plugins
from settings import parse_config
from shared.path_setup import PROJECT_ROOT as TIFFANY_ROOT
from shared.paths import RuntimePaths
from shared.plugins import PluginError, PluginSettings, discover_plugins

from tiffany_plugin_repeater import SERVICE, RepeaterConfig, install
from tiffany_plugin_repeater.service import RepeaterService

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PluginApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="repeater-api1-")
        self.paths = RuntimePaths.resolve(self.temporary.name)
        self.paths.initialize()
        self.paths.plugin_settings.write_bytes(PluginSettings(paths=(str(PROJECT_ROOT),)).encode())
        self.bot = Bot()
        self.bot.service(APP_PATHS, self.paths)

    async def asyncTearDown(self):
        self.assertTrue((await self.bot.stop("abort")).successful)
        self.temporary.cleanup()

    def config(self, text):
        self.paths.config.with_name("repeater.toml").write_text(text, encoding="utf-8")

    async def load(self):
        return await install_plugins(self.bot, self.paths, discover_plugins(self.paths))

    def test_manifest_is_discovered_with_the_correct_api1_entry(self):
        plan = discover_plugins(self.paths)
        self.assertEqual(len(plan.enabled), 1)
        spec = plan.enabled[0]
        self.assertEqual((spec.id, spec.version, spec.entry),
                         ("repeater", "0.3.1", "tiffany_plugin_repeater:install"))
        self.assertEqual(spec.root, PROJECT_ROOT)
        self.assertEqual(spec.requires, ())

    async def test_missing_configuration_registers_defaults_without_io(self):
        installed = await self.load()
        service = self.bot.services.get(SERVICE)
        self.assertEqual(installed.scopes[0].name, "plugins.repeater")
        self.assertEqual(service.config, RepeaterConfig())
        self.assertEqual(service.owner, installed.scopes[0].owner)
        self.assertIsNone(service.store._executor)
        self.assertIsNone(service.cache.session)
        self.assertFalse((self.paths.storage / "repeater").exists())
        self.assertFalse(self.paths.config.with_name("repeater.toml").exists())

    async def test_startup_creates_configuration_and_next_start_loads_edits(self):
        await self.load()
        register_onebot_fields(self.bot, "napcat")
        path = self.paths.config.with_name("repeater.toml")
        with self.assertLogs("tiffany_plugin_repeater.service", level="INFO") as logs:
            await self.bot.start()
        self.assertEqual(RepeaterConfig.load(path), RepeaterConfig())
        self.assertTrue(any(str(path) in message for message in logs.output))
        self.assertTrue((await self.bot.stop("abort")).successful)

        updated = '# 保留我的注释\nignored_user_ids = ["222222"]\ndelay_min_seconds = 7\ndelay_max_seconds = 12\n'
        self.config(updated)
        original = path.read_bytes()
        self.bot = Bot()
        self.bot.service(APP_PATHS, self.paths)
        await self.load()
        register_onebot_fields(self.bot, "napcat")
        await self.bot.start()
        service = self.bot.services.get(SERVICE)
        self.assertEqual(service.config.ignored_user_ids, ("222222",))
        self.assertEqual((service.config.delay_min_seconds, service.config.delay_max_seconds), (7, 12))
        self.assertEqual(path.read_bytes(), original)

    async def test_generated_configuration_preserves_programmatic_options(self):
        config = RepeaterConfig(ignored_user_ids=("222222",), delay_min_seconds=7, delay_max_seconds=12)
        install(self.bot.scope("plugins.repeater"), config)
        register_onebot_fields(self.bot, "napcat")
        await self.bot.start()
        self.assertEqual(RepeaterConfig.load(self.paths.config.with_name("repeater.toml")), config)

    async def test_config_creation_failure_warns_without_blocking_startup(self):
        await self.load()
        register_onebot_fields(self.bot, "napcat")
        with patch.object(RepeaterConfig, "write_if_missing", side_effect=PermissionError("read-only config")), \
             self.assertLogs("tiffany_plugin_repeater.service", level="WARNING") as logs:
            await self.bot.start()
        self.assertTrue(self.bot.services.get(SERVICE).active)
        self.assertTrue(any("read-only config" in message for message in logs.output))

    async def test_host_configuration_dispatch_owner_and_unload(self):
        self.config("repeat_threshold=2\ndelay_min_seconds=0\ndelay_max_seconds=0\n")
        installed = await self.load()
        service = self.bot.services.get(SERVICE)
        register_onebot_fields(self.bot, "napcat")
        client = SimpleNamespace(running=True, call=AsyncMock(return_value=SimpleNamespace(data={"message_id": 777})), reply=AsyncMock())
        await self.bot.start()
        self.assertTrue(self.bot.runtime.tasks.tasks_for("plugins.repeater"))
        for identifier, text in ((1, "原生插件接口测试"), (2, "原生插件接口测试")):
            raw = {"post_type": "message", "message_type": "group", "group_id": 5, "user_id": 2,
                   "self_id": 9, "message_id": identifier, "message": text}
            await self.bot.emit(Envelope("napcat", raw, client=client, kind="message", adapter_id="onebot:test", session_id="group:5"))
        await asyncio.gather(*tuple(service.pending.values()))
        client.call.assert_awaited_once()
        self.assertEqual(client.call.await_args.args[1]["message"][0]["data"]["text"], "原生插件接口测试")
        await installed.scopes[0].unload(mode="abort")
        self.assertFalse(self.bot.runtime.tasks.tasks_for("plugins.repeater"))
        self.assertIsNone(service.store._executor)
        self.assertIsNone(service.cache.session)
        with self.assertRaises(LookupError):
            self.bot.services.get(SERVICE)

    async def test_disabled_plugin_is_not_imported_or_registered(self):
        self.paths.plugin_settings.write_bytes(PluginSettings(paths=(str(PROJECT_ROOT),), disabled=("repeater",)).encode())
        with patch("hooks.plugins.import_module", side_effect=AssertionError("disabled entry imported")):
            installed = await self.load()
        self.assertFalse(installed.scopes)
        self.assertFalse(self.paths.config.with_name("repeater.toml").exists())
        with self.assertRaises(LookupError):
            self.bot.services.get(SERVICE)

    async def test_invalid_options_report_plugin_and_roll_back(self):
        self.config("cooldown_seconds=-1\n")
        with self.assertRaises(PluginError) as caught:
            await self.load()
        self.assertEqual((caught.exception.plugin, caught.exception.phase), ("repeater", "registration"))
        self.assertFalse(self.bot.scope("plugins.repeater").active)
        self.assertFalse(self.bot.dispatcher.handles_for_owner("plugins.repeater"))
        self.assertFalse((self.paths.storage / "repeater").exists())

    async def test_malformed_toml_fails_before_registration(self):
        self.config("answer_weights = [\n")
        with self.assertRaises(PluginError) as caught:
            await self.load()
        self.assertEqual((caught.exception.plugin, caught.exception.phase), ("repeater", "configuration"))
        self.assertFalse(self.bot.services.handles_for_owner("plugins.repeater"))

    async def test_setup_failure_is_identified_and_resources_are_closed(self):
        installed = await self.load()
        register_onebot_fields(self.bot, "napcat")
        target = self.paths.storage / "repeater"
        target.write_text("retain this file", encoding="utf-8")
        with self.assertRaises(OSError):
            await self.bot.start()
        failure = installed.startup_failure(self.bot.runtime)
        self.assertEqual((failure.plugin, failure.phase), ("repeater", "setup"))
        service = self.bot.services.get(SERVICE)
        self.assertIsNone(service.store._executor)
        self.assertIsNone(service.cache.session)
        self.assertEqual(target.read_text("utf-8"), "retain this file")

    async def test_host_check_validates_registration_without_starting_resources(self):
        # Environment attestation is the host's concern. Exercise its actual
        # offline check after that gate, with setup forbidden.
        with patch("deployment.plugin_environment.verify_prepared"), \
             patch.object(RepeaterService, "setup", side_effect=AssertionError("check initialized resources")):
            await check_registration(self.paths)
        self.assertFalse((self.paths.storage / "repeater").exists())
        self.assertFalse(self.paths.config.with_name("repeater.toml").exists())

    def test_standard_launcher_lists_plugin_from_another_working_directory(self):
        result = subprocess.run([sys.executable, "-B", str(TIFFANY_ROOT / "launcher.py"),
                                 "plugins", "list", "--home", str(self.paths.home)],
                                cwd=self.paths.home, capture_output=True, text=True, encoding="utf-8", timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("repeater\tenabled\tpending", result.stdout)
        self.assertIn(str(PROJECT_ROOT), result.stdout)

    async def test_standard_application_loads_plugin_automatically(self):
        config = parse_config({"adapter": {"type": "onebot_websocket", "platform": "napcat", "websocket": {"port": 0}},
                               "extensions": {"enabled": []}, "monitoring": {"enabled": False}})
        application_bot = Bot()
        task = None
        try:
            with patch("application.verify_prepared"), \
                 patch.object(TiffanyApplication, "_create_bot", return_value=application_bot):
                task = asyncio.create_task(TiffanyApplication._run(self.paths, config))
                async def wait_running():
                    while application_bot.runtime.state != RuntimeState.RUNNING:
                        if task.done():
                            await task
                            raise AssertionError("application exited before startup")
                        await asyncio.sleep(.005)
                # Cold SQLite initialization and tokenizer loading can exceed 3s on Windows.
                await asyncio.wait_for(wait_running(), 10)
                service = application_bot.services.get(SERVICE)
                self.assertEqual(service.owner, "plugins.repeater")
                self.assertTrue(service.active)
                self.assertEqual(RepeaterConfig.load(self.paths.config.with_name("repeater.toml")), service.config)
                await application_bot.stop("drain")
                self.assertEqual(await asyncio.wait_for(task, 3), 0)
        finally:
            await application_bot.stop("abort")
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_existing_corpus_survives_host_reload(self):
        # The scope owner changes from the old manual entry to API 1, while
        # persistence stays under storage/repeater with schema version 1.
        from tiffany_plugin_repeater.models import Content
        from tiffany_plugin_repeater.storage import Store
        directory = self.paths.storage / "repeater"
        store = Store(directory, RepeaterConfig())
        await store.open()
        try:
            await store.ensure("existing-group", "account", "group")
            await store.configure("existing-group", "preset", "话痨的")
            await store.learn("existing-group", Content.text("保留旧版的语料"), "2", "event-1", 100)
        finally:
            await store.close()
        await self.load()
        register_onebot_fields(self.bot, "napcat")
        await self.bot.start()
        service = self.bot.services.get(SERVICE)
        self.assertEqual((await service.store.settings("existing-group")).preset, "话痨的")
        self.assertEqual(len(await service.store.recent("existing-group")), 1)
        await self.bot.stop()
        db = sqlite3.connect(directory / "repeater.sqlite3")
        try:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
        finally:
            db.close()
