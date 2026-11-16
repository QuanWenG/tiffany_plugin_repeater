"""Platform independent content owned by this extension."""
import hashlib
import json
import re
from dataclasses import dataclass
from functools import lru_cache

URL = re.compile(r"https?://|www\.|(?:[\w-]+\.)+(?:com|cn|net|org|io)\b", re.IGNORECASE)
CONTROL = re.compile(r"[\x00-\x1f\x7f]")
EXPRESSION_PLACEHOLDER = re.compile(
    r"\[(?:表情|动画表情|商城表情)(?:\s*[:：]\s*(?:[^\[\]\r\n]|\[[^\[\]\r\n]*\])*)?\]"
    r"|【(?:表情|动画表情|商城表情)(?:\s*[:：]\s*[^【】\r\n]*)?】"
)
ANNOTATION = re.compile(
    r'''<\s*attachmentType\s*=\s*["']image/[^<>]+>|''' + EXPRESSION_PLACEHOLDER.pattern,
    re.IGNORECASE,
)
MENTION = re.compile(r"<@!?[^<>\s]+>|<@&[^<>\s]+>")


def clean_annotation(text):
    return ANNOTATION.sub("", text).strip()


@lru_cache(maxsize=8192)
def keywords(text: str) -> str:
    try:
        from jieba.analyse import extract_tags
    except ImportError:
        return re.sub(r"\s+", "", text).lower()[:24]
    tags = extract_tags(text, topK=2)
    return " ".join(tags) if tags else re.sub(r"\s+", "", text).lower()[:24]


@lru_cache(maxsize=32768)
def _plain_segments(segments):
    return " ".join(str(s.value) for s in segments if s.kind == "text")


@lru_cache(maxsize=32768)
def _dump_segments(segments):
    return json.dumps([[s.kind, s.value, s.sticker] for s in segments], ensure_ascii=False, separators=(",", ":"))


@lru_cache(maxsize=32768)
def _key_segments(segments):
    return hashlib.sha256(_dump_segments(segments).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Segment:
    kind: str
    value: str | int
    sticker: bool = False

    def __post_init__(self):
        if self.kind not in ("text", "image", "face"):
            raise ValueError("unsupported content segment")
        if self.kind == "face":
            if type(self.value) is not int or self.value < 0:
                raise ValueError("invalid face ID")
        elif not isinstance(self.value, str) or not self.value:
            raise ValueError("empty content segment")


@dataclass(frozen=True, slots=True)
class Content:
    segments: tuple[Segment, ...]

    def __post_init__(self):
        if not self.segments:
            raise ValueError("empty content")

    @classmethod
    def text(cls, value):
        return cls((Segment("text", value),))

    @property
    def plain(self):
        return _plain_segments(self.segments)

    @property
    def media(self):
        return any(s.kind != "text" for s in self.segments)

    @property
    def categories(self):
        return frozenset("text" if s.kind == "text" else "expression" if s.kind == "face" or s.sticker or str(s.value).endswith(".gif") else "media" for s in self.segments)

    @property
    def addressed(self):
        return self.plain.lstrip().startswith("龟龟")

    @property
    def words(self):
        return keywords(self.plain) if self.plain else "media:" + self.key

    @property
    def key(self):
        return _key_segments(self.segments)

    def dump(self):
        return _dump_segments(self.segments)

    @classmethod
    def load(cls, payload):
        return _load_content(cls, payload)

    def answer(self, addressed=False):
        if not self.addressed:
            return self
        if not addressed or len(self.plain) <= 6:
            return None
        result = []
        stripped = False
        for segment in self.segments:
            if segment.kind == "text" and not stripped:
                text = str(segment.value).lstrip().removeprefix("龟龟").lstrip(" ，,：:、！!？?。.")
                stripped = True
                if text:
                    result.append(Segment("text", text))
            else:
                result.append(segment)
        return Content(tuple(result)) if result else None


@lru_cache(maxsize=32768)
def _load_content(cls, payload):
    return cls(tuple(Segment(*s) for s in json.loads(payload)))


@dataclass(frozen=True, slots=True)
class GroupSettings:
    enabled: bool = True
    text_enabled: bool = True
    expression_enabled: bool = True
    media_enabled: bool = False
    preset: str = "沉默的"
    intelligence_mode: str | None = None

    def allows(self, content):
        return self.enabled and all(getattr(self, f"{category}_enabled") for category in content.categories)


@dataclass(frozen=True, slots=True)
class ReplyDecision:
    content: Content
    trigger_key: str = ""
    trigger_payload: str = ""
    trigger_id: str = ""
    history: tuple = ()
    raw_answer_key: str = ""
    mode: str = "normal"
    source: str = "legacy"
    probability: float = 1.0
    diagnostics: tuple = ()
    shadow: dict | None = None
    data_version: int = 0
    policy_version: int = 1
    source_expression: str = ""
    evidence_refs: tuple = ()

    @property
    def previous_key(self):
        return self.history[-1]["content_key"] if self.history else ""


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    message_id: str | None
    content: Content
    created: float
