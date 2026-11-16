"""One Tiffany service owns policy, resources, timers and its message hook."""
import asyncio
import json
import logging
import time
from dataclasses import replace

from core import RuntimeState
from fields import USER_ID

from .config import INTELLIGENCE_MODES, PRESETS
from .engine import Engine
from .feedback import (
    FeedbackCollector,
    FeedbackObservation,
    connection_key,
    reaction_observations,
)
from .graph import GraphWorker
from .messages import Incoming, MediaCache, parse_content
from .models import Content, ReplyDecision, keywords
from .storage import Store
from .transport import DeliveryError, Sender

logger = logging.getLogger(__name__)
DRINK = frozenset(("龟龟喝酒", "龟龟干杯", "龟龟继续喝"))
SOBER = frozenset(("龟龟醒酒", "龟龟醒一醒", "龟龟别喝了"))
MUTE = "龟龟闭嘴"
UNMUTE = "龟龟说话"
SOBER_REPLY = "唔~~刚刚发生什么了"
RETRACT_REPLY = "很抱歉我的头可能会不小心顶到人，我会尽快缩回去"
FALLBACK = ("艾斯比吧你是", "你先叫声龟父来听听", "智利有障碍", "脑子有问题", "你算个吧唧", "给你龟叠整笑了", "命令神龟你配吗老弟", "我先顶似你个似糖比")


class RepeaterService:
    def __init__(self, directory, config, owner, command_router=None, *, config_path=None):
        self.config, self.owner = config, owner
        self.config_path = config_path
        self.store = Store(directory, config)
        self.cache = MediaCache(directory / "media", config)
        self.engine = Engine(self.store, config)
        self.engine.cache = self.cache
        self.sender = Sender(self.cache)
        self.command_router = command_router
        self.runtime = None
        self.dashboard = None
        self.active = False
        self.pending = {}
        self.sober_tasks = {}
        self.muted_until = {}
        self.locks = {}
        self.maintenance = None
        self.graph = None
        self.feedback = FeedbackCollector(self.store)
        self.account_clients = {}
        self.broken_connections = set()

    async def setup(self, runtime):
        self.runtime = runtime
        try:
            if self.config_path is not None:
                try:
                    created = await asyncio.to_thread(self.config.write_if_missing, self.config_path)
                except OSError as error:
                    logger.warning("无法自动生成复读机配置 %s，继续使用已加载配置：%s", self.config_path, error)
                else:
                    if created:
                        logger.info("已创建复读机配置 %s，修改后重启生效", self.config_path)
            await self.store.open()
            await self.cache.open()
            await asyncio.to_thread(keywords, "分词词典初始化")
            self.graph = GraphWorker(self.store)
            if self.config.dashboard_enabled:
                from .dashboard import Dashboard
                self.dashboard = Dashboard(self)
                await self.dashboard.open()
        except BaseException:
            await self.teardown()
            raise

    async def start(self):
        self.active = True
        self.maintenance = self.spawn(self._maintenance(), "maintenance")
        self.spawn(self._intelligence_maintenance(), "intelligence")

    def spawn(self, coroutine, label):
        return self.runtime.tasks.spawn(coroutine, name="repeater:" + label, owner=self.owner,
                                        critical=label == "maintenance",
                                        failure_policy="disable_owner" if label == "maintenance" else "abort_runtime")

    async def stop(self, mode="drain"):
        self.active = False
        if self.runtime is not None:
            abandoned = await self.runtime.tasks.cancel_owner(self.owner)
            if abandoned:
                raise RuntimeError("repeater tasks did not stop: " + ", ".join(abandoned))
        self.pending.clear()
        self.sober_tasks.clear()
        self.muted_until.clear()
        self.engine.drunk_until.clear()
        self.feedback.pending.clear()
        self.feedback.lost_groups.clear()
        if self.store._accepting:
            await self.store.invalidate_observations()

    async def teardown(self):
        if self.dashboard:
            await self.dashboard.close()
            self.dashboard = None
        if self.graph:
            await self.graph.close()
            self.graph = None
        await self.cache.close()
        await self.store.close()

    def lock(self, key):
        return self.locks.setdefault(key, asyncio.Lock())

    def _running(self):
        return self.active and self.runtime.state == RuntimeState.RUNNING

    def is_admin(self, event):
        if event.author in self.config.admin_ids:
            return True
        if self.config.allow_group_admin and event.group_admin:
            return True
        permission = getattr(self.command_router, "permission", None)
        if permission is not None:
            native_user = event.ctx.resolve(USER_ID)
            return permission.allows((event.ctx.envelope.adapter_id, "group", type(native_user), native_user))
        return False

    def _other_command(self, plain):
        if plain.casefold() == "ping":
            return True
        router = self.command_router
        if router is None or not plain.startswith(router.prefix):
            return False
        head = plain[len(router.prefix):].split(None, 1)
        return bool(head and head[0].casefold() in router.table)

    async def handle(self, ctx):
        # The runtime has already admitted these events; graceful drain must
        # still commit their learning before the service is closed.
        if not self.active or self.runtime.state not in (RuntimeState.RUNNING, RuntimeState.STOPPING_DRAIN):
            return
        event = Incoming.read(ctx)
        if event is None:
            return
        if event.sender_bot or event.author in self.config.ignored_user_ids:
            return
        async with self.lock(event.group_key):
            await self._track_client(event.namespace, ctx.client)
            self.sender.bind(event)
            await self.store.ensure(event.group_key, event.namespace, event.target)
            plain = event.plain.strip()
            command = plain.removeprefix(self.config.command_prefix).strip()
            words = command.split()
            ours = command in DRINK | SOBER | {"龟龟顶人", "龟龟缩头", MUTE, UNMUTE} or bool(words and words[0].casefold() == "repeater" and plain.startswith(self.config.command_prefix))
            if ours and not event.command_addressed:
                # A command to another account must neither execute nor become
                # a prompt in this account's learning chain. Leave other hooks alone.
                await self.store.barrier(event.group_key)
                return
            if ours and event.command_safe:
                if not await self.store.claim(event.event_key, time.time()):
                    return
                await self.store.barrier(event.group_key)
                await self._command(event, command, words)
                ctx.stop()
                return
            if ours or plain.startswith(self.config.command_prefix) or self._other_command(plain):
                await self.store.barrier(event.group_key)
                return
            settings = await self.store.settings(event.group_key)
            if not self.config.enabled or not settings.enabled:
                await self.store.barrier(event.group_key)
                return
            config = self.engine.effective(event.group_key, settings)
            mode = settings.intelligence_mode or config.intelligence_mode
            now = time.time()
            history, _ = await self.store.history(event.group_key, now) if mode != "off" else ([], 0)
            quoted = None
            parsed_event = event
            if event.reply_to and mode != "off":
                episode = await self.store.receipt_episode(event.group_key, event.reply_to)
                if episode is not None:
                    await self.feedback.capture(FeedbackObservation(event.group_key, event.reply_to, event.author, "quote", event.message_id or event.event_key, True, now))
                    await self.store.barrier(event.group_key)
                    return
                quoted = await self.store.quoted_human(event.group_key, event.reply_to, now)
                if quoted is None:
                    await self.feedback.capture(FeedbackObservation(event.group_key, event.reply_to, event.author, "quote", event.message_id or event.event_key, True, now))
                    await self.store.barrier(event.group_key)
                    return
                if not await self.engine.allowed(event.group_key, Content.load(quoted["payload"])):
                    await self.store.barrier(event.group_key)
                    return
                parsed_event = replace(event, segments=[s for s in event.segments if s.get("type") != "reply"])
            content = await parse_content(parsed_event, self.cache, config, settings)
            if content is None:
                await self.store.barrier(event.group_key)
                return
            reply = None if self.mute_remaining(event.group_key) else await self.engine.decide(event, content, history, mode=mode)
            if not await self.store.observe(event, content, history, time.time(), mode=mode, quoted=quoted):
                return
            from .engine import Activity
            activity = self.engine.activity.setdefault(event.group_key, Activity(first=now))
            activity.count += 1
            activity.last = now
            if reply is not None and reply.mode == "shadow":
                await self.store.begin_episode(event.group_key, reply, connection_key(ctx.client), time.time())
                reply = None
            if reply is not None and event.group_key not in self.pending and len(self.pending) < self.config.max_pending_sends:
                delay = self.engine.rng.uniform(config.delay_min_seconds, config.delay_max_seconds)
                task = self.spawn(self._delayed(event, reply, delay), "reply")
                self.pending[event.group_key] = task

    async def _delayed(self, event, content, delay):
        task = asyncio.current_task()
        try:
            await asyncio.sleep(delay)
            async with self.lock(event.group_key):
                await self._send(event.group_key, content, event.ctx)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("延迟复读发送失败")
        finally:
            if self.pending.get(event.group_key) is task:
                self.pending.pop(event.group_key, None)

    async def _send(self, key, content, ctx=None, force=False):
        decision = content if isinstance(content, ReplyDecision) else ReplyDecision(content, mode="forced" if force else "proactive" if ctx is None else "normal", source="forced" if force else "proactive")
        if decision.mode == "shadow":
            return False
        content = decision.content
        if not self._running() or self.mute_remaining(key):
            return False
        bundle = await self.store.selection_bundle(key, 0)
        if not self.engine._static_allowed(bundle, content):
            return False
        settings = bundle["settings"]
        config = self.engine.effective(key, settings)
        mode = settings.intelligence_mode or config.intelligence_mode
        route = self.sender.routes.get(key)
        client = self.sender.clients.get(route.namespace) if route else None
        reservation = await self.store.reserve(key, content, time.time(), config, force,
                                               decision=decision if mode != "off" else None,
                                               connection_id=connection_key(client))
        if reservation is None:
            return False
        token, episode_id = reservation if mode != "off" else (reservation, None)
        before = self.sender.delivered.get(key, 0)
        complete = False
        try:
            if mode != "off":
                async def persist(receipt):
                    await self.store.add_receipt(episode_id, key, receipt)
                    if receipt.message_id:
                        await self.feedback.flush(group_key=key, target_id=receipt.message_id)
                await self.sender.send(key, content, ctx, on_receipt=persist)
            else:
                await self.sender.send(key, content, ctx)
            complete = True
        except DeliveryError:
            logger.exception("复读发送失败；已发送的部分保留回执")
        finally:
            success = self.sender.delivered.get(key, 0) > before
            now = time.time()
            await asyncio.shield(self.store.finish(token, now, success, episode_id=episode_id, complete=complete))
            if success:
                self.engine.sent(key, content, now)
        return success

    async def _track_client(self, namespace, client):
        previous = self.account_clients.get(namespace)
        if previous is not None and previous is not client:
            await self._break_account(namespace)
            self.broken_connections.discard(connection_key(previous))
        self.account_clients[namespace] = client
        self.sender.clients[namespace] = client

    async def _break_account(self, namespace):
        def run(db):
            with db:
                db.execute("UPDATE groups SET barrier=1 WHERE namespace=?", (namespace,))
                db.execute("UPDATE learning_state SET tail='[]',segment=segment+1 WHERE group_key IN (SELECT key FROM groups WHERE namespace=?)", (namespace,))
                db.execute("UPDATE reply_episodes SET observable=0 WHERE settled=0 AND group_key IN (SELECT key FROM groups WHERE namespace=?)", (namespace,))
        await self.store.call(run)

    async def handle_notice(self, ctx):
        if not self._running() or not self.config.enabled:
            return
        observations = reaction_observations(ctx)
        if not observations:
            return
        key = observations[0].group_key
        async with self.lock(key):
            namespace = json.dumps([ctx.envelope.adapter_id, str(ctx.raw["self_id"])], separators=(",", ":"))
            await self._track_client(namespace, ctx.client)
            settings = await self.store.settings(key)
            if not settings.enabled or (settings.intelligence_mode or self.config.intelligence_mode) == "off":
                return
            for observation in observations:
                if observation.actor not in self.config.ignored_user_ids:
                    await self.feedback.capture(observation)

    async def _intelligence_maintenance(self):
        while self.active:
            await asyncio.sleep(5)
            if not self._running():
                continue
            try:
                for namespace, client in tuple(self.account_clients.items()):
                    connection = connection_key(client)
                    if not getattr(client, "running", True) and connection not in self.broken_connections:
                        await self._break_account(namespace)
                        self.broken_connections.add(connection)
                await self.feedback.flush()
                await self.store.settle_feedback(time.time())
                await self.store.cleanup_learning(time.time())
                await self.graph.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.store.metrics["intelligence_background_failures"] += 1
                logger.exception("智能学习维护失败，将重试")

    def cancel_reply(self, key):
        task = self.pending.pop(key, None)
        if task:
            task.cancel()

    def mute_remaining(self, key, now=None):
        deadline = self.muted_until.get(key)
        if deadline is None:
            return 0
        remaining = deadline - (time.monotonic() if now is None else now)
        if remaining <= 0:
            self.muted_until.pop(key, None)
            return 0
        return remaining

    def sober(self, key):
        self.engine.drunk_until.pop(key, None)
        task = self.sober_tasks.pop(key, None)
        if task:
            task.cancel()

    async def _auto_sober(self, key, deadline):
        task = asyncio.current_task()
        try:
            await asyncio.sleep(max(0, deadline - time.time()))
            async with self.lock(key):
                if self.engine.drunk_until.get(key) != deadline:
                    return
                self.engine.drunk_until.pop(key, None)
                settings = await self.store.settings(key)
                if self._running() and self.config.enabled and settings.enabled and not self.mute_remaining(key):
                    await self.sender.notice(key, SOBER_REPLY)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("自动醒酒提醒失败")
        finally:
            if self.sober_tasks.get(key) is task:
                self.sober_tasks.pop(key, None)

    async def _command(self, event, command, words):
        key, ctx = event.group_key, event.ctx
        settings = await self.store.settings(key)
        if command == MUTE:
            self.cancel_reply(key)
            duration = self.config.mute_duration_seconds
            self.muted_until[key] = time.monotonic() + duration
            await ctx.reply("哼，闭嘴就闭嘴，在你说‘龟龟说话‘前我不会原谅你的")
            return
        if command == UNMUTE:
            self.muted_until.pop(key, None)
            if self.config.enabled and settings.enabled:
                await ctx.reply("看在你诚心诚意，我就原谅你吧")
            else:
                await ctx.reply("已解除本群静音；复读机仍处于关闭状态。")
            return
        # Explicit administration remains available while everyday interactions
        # (including forced replies and drink/sober acknowledgements) stay silent.
        if self.mute_remaining(key) and (not words or words[0].casefold() != "repeater"):
            return
        if command == "龟龟缩头":
            try:
                await self.sender.recall(key)
            except Exception:
                logger.exception("复读消息撤回失败")
            await ctx.reply(RETRACT_REPLY)
            return
        if command in DRINK | SOBER:
            if self.config.drink_admin_only and not self.is_admin(event):
                await ctx.reply("只有管理员可以让龟龟喝酒或醒酒。")
            elif command in SOBER:
                self.sober(key)
                await ctx.reply(SOBER_REPLY)
            elif not self.config.enabled or not settings.enabled:
                await ctx.reply("本群复读机已关闭，请先用 /repeater on 开启。")
            else:
                self.sober(key)
                deadline = time.time() + self.config.drunk_duration_seconds
                self.engine.drunk_until[key] = deadline
                self.sober_tasks[key] = self.spawn(self._auto_sober(key, deadline), "sober")
                await ctx.reply("龟龟干杯！")
            return
        if command == "龟龟顶人":
            if not self.config.enabled or not settings.enabled or not settings.text_enabled:
                await ctx.reply("本群文本复读未开启。")
                return
            if key in self.pending:
                await ctx.reply("龟龟暂时没有可发送的接话。")
                return
            reply = await self.engine.forced(event)
            recent = await self.store.recent(key, 1, sent=True)
            if reply is None or recent and recent[0]["content_key"] == reply.key:
                candidates = [Content.text(text) for text in FALLBACK]
                candidates = [c for c in candidates if await self.engine.allowed(key, c)]
                reply = self.engine.rng.choice(candidates) if candidates else None
            if reply is None or not await self._send(key, reply, ctx, force=True):
                await ctx.reply("龟龟暂时没有可发送的接话。")
            return
        if not self.is_admin(event):
            await ctx.reply("只有管理员可以管理复读机。")
            return
        args = words[1:]
        verb = args[0].casefold() if args else "status"
        if verb == "status" and len(args) <= 1:
            rows = await self.store.recent(key, self.config.max_messages_per_group)
            await ctx.reply(f"复读机：{'开启' if settings.enabled else '关闭'}\n文本：{'开启' if settings.text_enabled else '关闭'}；表情：{'开启' if settings.expression_enabled else '关闭'}；图片：{'开启' if settings.media_enabled else '关闭'}\n频率：{settings.preset}；语料：{len(rows)} 条\n醉酒剩余：{int(self.engine.remaining(key))} 秒\n静音剩余：{int(self.mute_remaining(key))} 秒")
        elif verb == "frequency" and len(args) <= 2:
            if len(args) == 1:
                await ctx.reply("本群频率：" + settings.preset)
            elif args[1] not in PRESETS:
                await ctx.reply("可选频率：" + "、".join(PRESETS))
            else:
                await self.store.configure(key, "preset", args[1])
                await ctx.reply("本群频率已设为：" + args[1])
        elif verb == "intelligence" and len(args) <= 2:
            if len(args) == 1:
                await ctx.reply("本群智能策略：" + (settings.intelligence_mode or self.config.intelligence_mode))
            elif args[1] not in INTELLIGENCE_MODES:
                await ctx.reply("可选策略：auto / observe / off")
            else:
                self.cancel_reply(key)
                await self.store.configure(key, "intelligence_mode", args[1])
                if args[1] == "off":
                    self.feedback.discard_group(key)
                await ctx.reply("本群智能策略已设为：" + args[1])
        elif verb in ("on", "off") and len(args) == 1:
            await self.store.configure(key, "enabled", verb == "on")
            if verb == "off":
                self.cancel_reply(key)
                self.sober(key)
            await ctx.reply("本群复读机已" + ("开启。" if verb == "on" else "关闭。"))
        elif verb in ("text", "expression", "media") and len(args) == 2 and args[1] in ("on", "off", "开启", "关闭"):
            await self.store.configure(key, verb + "_enabled", args[1] in ("on", "开启"))
            await ctx.reply("本群分类设置已更新。")
        elif verb in ("forget", "forget_keyword") and len(args) == 1:
            self.cancel_reply(key)
            changed = await self.store.forget(key, verb == "forget_keyword")
            self.feedback.discard_group(key)
            await ctx.reply("已处理最近一次复读内容。" if changed else "暂无可处理的复读记录。")
        elif verb == "clear" and args == ["clear", "confirm"]:
            self.cancel_reply(key)
            await self.store.clear(key)
            self.feedback.discard_group(key)
            self.engine.activity.pop(key, None)
            # Cache garbage collection runs in maintenance, after active parses settle.
            await ctx.reply("本群语料已清空，开关和频率设置保留。")
        else:
            await ctx.reply("用法：/repeater status | intelligence [auto/observe/off] | frequency [沉默的/活跃的/话痨的] | on/off | text/expression/media on/off | forget | forget_keyword | clear confirm")

    async def _maintenance(self):
        last_cleanup = 0
        try:
            while self.active:
                await asyncio.sleep(self.config.proactive_interval_seconds)
                if not self._running():
                    continue
                now = time.time()
                if now - last_cleanup >= 86400:
                    await self.store.cleanup(now)
                    # Do not remove a just-downloaded image still inside a hook.
                    if not any(lock.locked() for lock in self.locks.values()):
                        await self.cache.prune(await self.store.media_references())
                    last_cleanup = now
                for key in tuple(self.engine.activity):
                    if key in self.pending or self.mute_remaining(key):
                        continue
                    async with self.lock(key):
                        content = await self.engine.proactive(key, now)
                        if content and await self._send(key, content):
                            break
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("复读机后台任务异常")
            # A failed background service must become visible, not silently die.
            self.active = False
            raise
