"""Check the dashboard in a local headless Chromium browser, without installing one."""
import argparse
import asyncio
import base64
import json
import os
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp
from websockets.asyncio.client import connect

from tiffany_plugin_repeater.config import RepeaterConfig
from tiffany_plugin_repeater.dashboard import Dashboard
from tiffany_plugin_repeater.engine import Engine
from tiffany_plugin_repeater.feedback import FeedbackCollector
from tiffany_plugin_repeater.models import Content, DeliveryReceipt
from tiffany_plugin_repeater.storage import Store


async def run(browser, output):
    output.mkdir(parents=True, exist_ok=True)
    config = RepeaterConfig(dashboard_enabled=True, dashboard_port=0, dashboard_token="local-browser-fixture-token")
    with tempfile.TemporaryDirectory(prefix="repeater-browser-") as directory:
        directory = Path(directory)
        store = Store(directory, config)
        await store.open()
        service = SimpleNamespace(store=store, config=config, engine=Engine(store, config, random.Random(1)),
                                  active=True, pending={}, sober_tasks={}, feedback=FeedbackCollector(store))
        dashboard = Dashboard(service)
        process = None
        try:
            await store.ensure("演示群", "demo-account", "demo-group")
            now = time.time()
            index = 0
            for answer in ("建议老板自己干", "明天还得继续打卡"):
                for actor in range(3):
                    await store.barrier("演示群")
                    for text in ("今天又要加班", answer):
                        event = SimpleNamespace(group_key="演示群", namespace="demo-account", event_key=f"demo:{index}",
                                                message_id=str(index), author=str(actor), reply_to=None)
                        history, _ = await store.history("演示群", now)
                        await store.observe(event, Content.text(text), history, now)
                        index += 1
            decision = await service.engine.decide(event, Content.text("今天又要加班"))
            episode = await store.begin_episode("演示群", decision, "browser-demo", now)
            await store.add_receipt(episode, "演示群", DeliveryReceipt("demo-receipt", decision.content, now))
            await store.finish_episode(episode, True)
            await store.record_feedback("演示群", "demo-receipt", "参与者甲", "reaction", "128", True, now + 1)
            await store.record_feedback("演示群", "demo-receipt", "参与者乙", "quote", "demo-quote", True, now + 2)
            await dashboard.open()
            profile = directory / "browser"
            process = subprocess.Popen([str(browser), "--headless=new", "--disable-gpu", "--no-first-run",
                                        "--disable-background-networking", "--disable-sync", "--remote-debugging-port=0",
                                        "--remote-allow-origins=*",
                                        "--remote-debugging-address=127.0.0.1", "--window-size=1440,1000",
                                        "--user-data-dir=" + str(profile), "about:blank"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            port_file = profile / "DevToolsActivePort"
            for _ in range(200):
                if port_file.exists():
                    break
                await asyncio.sleep(.05)
            port = int(port_file.read_text().splitlines()[0])
            async with aiohttp.ClientSession() as session:
                # Edge can replace its startup tab while applying a fresh
                # profile; create a target owned by this check explicitly.
                async with session.put(f"http://127.0.0.1:{port}/json/new?about:blank") as response:
                    target = await response.json()
                async with connect(target["webSocketDebuggerUrl"], max_size=16 * 1024 * 1024) as ws:
                    sequence = 0
                    errors = []
                    async def cdp(method, params=None):
                        nonlocal sequence
                        sequence += 1
                        await ws.send(json.dumps({"id": sequence, "method": method, "params": params or {}}))
                        while True:
                            packet = json.loads(await asyncio.wait_for(ws.recv(), 10))
                            if packet.get("method") == "Runtime.exceptionThrown":
                                errors.append(packet["params"])
                            if packet.get("id") == sequence:
                                assert "error" not in packet, packet
                                return packet.get("result", {})
                    async def evaluate(expression):
                        result = await cdp("Runtime.evaluate", {"expression": expression, "returnByValue": True, "awaitPromise": True})
                        assert "exceptionDetails" not in result, result
                        return result["result"].get("value")
                    async def until(expression):
                        for _ in range(100):
                            if await evaluate(expression):
                                return
                            await asyncio.sleep(.05)
                        raise AssertionError(expression)
                    await cdp("Runtime.enable")
                    await cdp("Page.enable")
                    await cdp("Page.navigate", {"url": f"http://127.0.0.1:{dashboard.port}/"})
                    await until("document.querySelectorAll('nav button').length === 9")
                    await evaluate("document.querySelector('#token').value=" + json.dumps(config.dashboard_token) + ";document.querySelector('#login').requestSubmit()")
                    await until("document.querySelector('#summary').textContent.includes('运行概览')")
                    for label in ("聊天证据", "回答与依据", "群友互动", "相近表达", "语料查询"):
                        await evaluate("Array.from(document.querySelectorAll('nav button')).find(b=>b.textContent===" + json.dumps(label) + ").click()")
                        await until("document.querySelector('#summary').textContent.includes(" + json.dumps(label) + ")")
                        assert not await evaluate("document.querySelector('#error').textContent")
                        if label == "回答与依据":
                            await evaluate("document.querySelector('#output button').click()")
                            await until("!document.querySelector('#detail').hidden")
                            assert await evaluate("document.querySelector('#detail').textContent.includes('feedback_multiplier')")
                            screenshot = await cdp("Page.captureScreenshot", {"captureBeyondViewport": True})
                            (output / "dashboard-decisions.png").write_bytes(base64.b64decode(screenshot["data"]))
                    assert not errors, errors
                    (output / "dashboard-browser.json").write_text(json.dumps({"passed": True, "browser": str(browser),
                        "views": 6, "detail": True, "javascript_errors": errors}, ensure_ascii=False, indent=2), encoding="utf-8")
                    print("Browser checks passed")
        finally:
            if process:
                process.terminate()
                _, stderr = await asyncio.to_thread(process.communicate, timeout=10)
                if process.returncode:
                    print(stderr.decode(errors="replace")[-5000:], file=sys.stderr)
                # Chromium children release their temporary profile asynchronously.
                await asyncio.sleep(.5)
            await dashboard.close()
            await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", type=Path, default=Path("C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"))
    parser.add_argument("--output", type=Path, default=Path("audit"))
    args = parser.parse_args()
    if not args.browser.is_file():
        parser.error("Pass --browser with an installed Chromium browser")
    asyncio.run(run(args.browser, args.output))
