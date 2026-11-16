"""Validated native options; loading configuration never creates resources."""
import json
import math
import tomllib
from dataclasses import dataclass, fields, replace
from pathlib import Path

PRESETS = ("沉默的", "活跃的", "话痨的")
INTELLIGENCE_MODES = ("auto", "observe", "off")

CONFIG_COMMENTS = {
    "enabled": "插件总开关；enabled_default 为新群的默认开关。",
    "min_text_length": "可学习的文字长度范围。",
    "answer_threshold": "回答所需的学习次数；answer_weights 对应各次数的抽样权重。",
    "cross_group_threshold": "同一账户下，文字至少在多少个不同群学习后允许跨群复用。",
    "repeat_threshold": "连续相同消息达到此次数时跟随复读，至少为 2。",
    "topics_size": "用于近期话题判断的消息条数。",
    "duplicate_window": "避免重复最近多少条已发送的内容。",
    "cooldown_seconds": "每群两次发言的最短间隔，单位秒。",
    "delay_min_seconds": "普通接话在最小/最大延迟间随机等待，单位秒；两个机器人可设置不同区间。",
    "burst_window_seconds": "每群在此时间窗口内最多发送 burst_limit 次，单位秒。",
    "proactive_interval_seconds": "检查沉默后主动发言的周期，单位秒；多个实例可设置不同周期。",
    "proactive_delay_seconds": "主动发言的基础沉默时长，单位秒；另按群活跃度追加等待。",
    "proactive_recent_messages": "主动发言从最近多少条消息中选择。",
    "max_messages_per_group": "每群原始消息保留上限。",
    "stale_relation_days": "低频陈旧关系的清理期限，单位天。",
    "multimodal_enabled": "允许学习图片和表情；普通图片还需管理员在群内开启。",
    "max_media_segments": "单条消息允许的媒体段数量。",
    "max_image_bytes": "单张图片大小上限，单位字节。",
    "media_timeout_seconds": "媒体下载超时，单位秒。",
    "drunk_duration_seconds": "喝酒状态持续时间，单位秒。",
    "drink_admin_only": "是否仅允许管理员使用喝酒和醒酒命令。",
    "mute_duration_seconds": "龟龟闭嘴的默认静音时长，单位秒；龟龟说话可提前解除。",
    "admin_ids": "插件管理员 ID；OneBot 填 QQ 号字符串，QQ 官方填 OpenID。",
    "allow_group_admin": "是否允许 OneBot 群主/管理员管理插件。",
    "ignored_user_ids": '忽略这些账号的消息，不学习、不触发接话；如 ["另一个机器人的QQ号"]。',
    "command_prefix": "管理命令前缀。",
    "max_pending_sends": "待发送任务数量上限。",
    "max_storage_operations": "排队存储操作数量上限。",
    "dashboard_enabled": "只读语料观察台；开启前需设置至少 16 字符的 dashboard_token。",
    "dashboard_host": "观察台仅允许监听本机地址。",
    "dashboard_port": "观察台监听端口，多个实例需使用不同端口。",
    "intelligence_mode": "auto：证据达标后选句；observe：只计算和观察；off：使用原检索。",
    "evidence_retention_days": "人类聊天证据保留期限，单位天。",
    "max_evidence_per_group": "每群人类聊天证据保留上限。",
    "feedback_window_seconds": "机器人回答的互动收集窗口，单位秒。",
    "feedback_retention_days": "互动记录保留期限，单位天。",
    "max_reply_episodes_per_group": "每群回答决策记录保留上限。",
}


@dataclass(frozen=True, slots=True)
class RepeaterConfig:
    enabled: bool = True
    enabled_default: bool = True
    min_text_length: int = 2
    max_text_length: int = 120
    answer_threshold: int = 3
    answer_weights: tuple[int, ...] = (7, 23, 70)
    cross_group_threshold: int = 2
    repeat_threshold: int = 3
    topics_size: int = 16
    duplicate_window: int = 10
    cooldown_seconds: float = 5
    delay_min_seconds: float = 2
    delay_max_seconds: float = 5
    burst_window_seconds: float = 180
    burst_limit: int = 3
    proactive_interval_seconds: float = 60
    proactive_delay_seconds: float = 600
    proactive_activity_factor: float = 5
    proactive_recent_messages: int = 100
    max_messages_per_group: int = 10000
    stale_relation_days: int = 90
    multimodal_enabled: bool = True
    max_media_segments: int = 4
    max_image_bytes: int = 10 * 1024 * 1024
    media_timeout_seconds: float = 15
    drunk_duration_seconds: float = 300
    drink_admin_only: bool = False
    mute_duration_seconds: float = 1800
    admin_ids: tuple[str, ...] = ()
    allow_group_admin: bool = True
    ignored_user_ids: tuple[str, ...] = ()
    command_prefix: str = "/"
    max_pending_sends: int = 256
    max_storage_operations: int = 128
    dashboard_enabled: bool = False
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 9470
    dashboard_token: str = ""
    intelligence_mode: str = "auto"
    evidence_retention_days: int = 90
    max_evidence_per_group: int = 20000
    feedback_window_seconds: float = 1800
    feedback_retention_days: int = 30
    max_reply_episodes_per_group: int = 5000

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(field.default, bool) and type(value) is not bool:
                raise ValueError(f"{field.name} must be a boolean")
            if type(field.default) in (int, float):
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError(f"{field.name} must be a finite nonnegative number")
                if field.type is int and type(value) is not int:
                    raise ValueError(f"{field.name} must be an integer")
        for name in ("admin_ids", "ignored_user_ids"):
            values = getattr(self, name)
            if not isinstance(values, (list, tuple)) or any(type(v) not in (int, str) or str(v) == "" for v in values):
                raise ValueError(f"{name} must contain native IDs")
            object.__setattr__(self, name, tuple(str(v) for v in values))
        if not isinstance(self.answer_weights, (tuple, list)) or not self.answer_weights:
            raise ValueError("answer_weights must be a nonempty list")
        if any(type(w) is not int or w < 0 for w in self.answer_weights) or sum(self.answer_weights) == 0:
            raise ValueError("answer_weights must contain nonnegative integer weights")
        object.__setattr__(self, "answer_weights", tuple(self.answer_weights))
        if self.answer_threshold < len(self.answer_weights):
            raise ValueError("answer_threshold must cover answer_weights")
        if not 1 <= self.min_text_length <= self.max_text_length <= 4000:
            raise ValueError("invalid text length bounds")
        if self.delay_min_seconds > self.delay_max_seconds:
            raise ValueError("delay_min_seconds exceeds delay_max_seconds")
        for name in ("cross_group_threshold", "repeat_threshold", "topics_size", "duplicate_window", "burst_limit",
                     "proactive_recent_messages", "max_messages_per_group", "stale_relation_days", "max_media_segments",
                     "max_image_bytes", "max_pending_sends", "max_storage_operations",
                     "evidence_retention_days", "max_evidence_per_group", "feedback_retention_days",
                     "max_reply_episodes_per_group"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        for name in ("burst_window_seconds", "proactive_interval_seconds", "media_timeout_seconds", "drunk_duration_seconds",
                     "mute_duration_seconds", "feedback_window_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.repeat_threshold < 2 or self.max_image_bytes > 100 * 1024 * 1024:
            raise ValueError("invalid repeat threshold or media size")
        if not isinstance(self.command_prefix, str) or not self.command_prefix or any(c.isspace() for c in self.command_prefix):
            raise ValueError("command_prefix must be nonempty without whitespace")
        if not isinstance(self.dashboard_token, str) or not 0 <= self.dashboard_port <= 65535:
            raise ValueError("invalid dashboard configuration")
        if self.dashboard_host not in ("127.0.0.1", "::1", "localhost"):
            raise ValueError("dashboard_host must be loopback")
        if self.dashboard_enabled and len(self.dashboard_token) < 16:
            raise ValueError("dashboard_token needs at least 16 characters")
        if self.intelligence_mode not in INTELLIGENCE_MODES:
            raise ValueError("intelligence_mode must be auto, observe or off")

    @classmethod
    def from_mapping(cls, data):
        if not isinstance(data, dict):
            raise ValueError("repeater configuration must be a table")
        unknown = set(data) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown repeater options: {sorted(unknown)}")
        return cls(**data)

    @classmethod
    def load(cls, path: str | Path):
        with Path(path).open("rb") as stream:
            return cls.from_mapping(tomllib.load(stream))

    def write_if_missing(self, path: str | Path) -> bool:
        """Export startup options with comments without replacing an existing file."""
        lines = ["# Tiffany 复读机配置；修改后重启 Tiffany 生效。",
                 "# 此文件仅在缺失时自动生成，已有配置不会覆盖。",
                 "# 活跃的/话痨的频率预设会缩短等待；喝酒时普通接话延迟最多约 1～2 秒。"]
        for field in fields(self):
            comment = CONFIG_COMMENTS.get(field.name)
            if comment:
                lines.extend(("", "# " + comment))
            # These options only contain TOML-compatible JSON scalar/array types.
            # Keep Unicode literal (JSON surrogate pairs aren't valid TOML),
            # while escaping DEL, which TOML forbids inside basic strings.
            value = json.dumps(getattr(self, field.name), ensure_ascii=False).replace("\x7f", "\\u007f")
            lines.append(f"{field.name} = {value}")
        document = "\n".join(lines) + "\n"
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            stream = path.open("x", encoding="utf-8", newline="\n")
        except FileExistsError:
            return False
        with stream:
            stream.write(document)
        return True

    def effective(self, preset: str, drunk: bool = False):
        if preset not in PRESETS:
            raise ValueError("unknown frequency preset")
        factor = (1, 2, 4)[PRESETS.index(preset)]
        threshold = self.answer_threshold if factor == 1 else min(self.answer_threshold, 2 if factor == 2 else 1)
        weights = self.answer_weights if factor == 1 else ((30, 70) if threshold == 2 else (1,))
        options = dict(answer_threshold=threshold, answer_weights=weights,
                       repeat_threshold=self.repeat_threshold if factor == 1 else min(self.repeat_threshold, 3 if factor == 2 else 2),
                       cooldown_seconds=self.cooldown_seconds / factor,
                       delay_min_seconds=self.delay_min_seconds / factor,
                       delay_max_seconds=self.delay_max_seconds / factor,
                       burst_limit=self.burst_limit * factor,
                       proactive_delay_seconds=self.proactive_delay_seconds / factor,
                       proactive_activity_factor=self.proactive_activity_factor / factor)
        if drunk:
            options.update(min_text_length=1, answer_threshold=1, answer_weights=(1,),
                           repeat_threshold=2, cooldown_seconds=min(options["cooldown_seconds"], 2),
                           delay_min_seconds=min(options["delay_min_seconds"], 1),
                           delay_max_seconds=min(options["delay_max_seconds"], 2),
                           burst_limit=max(options["burst_limit"], 6))
        return replace(self, **options)
