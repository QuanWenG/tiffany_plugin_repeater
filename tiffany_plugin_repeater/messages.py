"""Read Tiffany raw events directly; cache only validated image bytes."""
import asyncio
import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import aiohttp
from fields import GROUP_ID, MESSAGE_ID, MESSAGE_TYPE, SELF_ID, USER_ID

from .models import (
    ANNOTATION,
    CONTROL,
    EXPRESSION_PLACEHOLDER,
    MENTION,
    URL,
    Content,
    Segment,
)


@dataclass(slots=True)
class Incoming:
    ctx: object
    namespace: str
    group_key: str
    target: str | int
    author: str
    event_key: str
    protocol: str
    segments: list
    plain: str
    command_safe: bool
    sender_bot: bool
    group_admin: bool
    message_id: str | None = None
    reply_to: str | None = None
    command_addressed: bool = True

    @classmethod
    def read(cls, ctx):
        if ctx.resolve(MESSAGE_TYPE) != "group":
            return None
        target, author = ctx.resolve(GROUP_ID), ctx.resolve(USER_ID)
        if target in (None, "") or author in (None, "") or ctx.client is None:
            return None
        raw = ctx.raw
        self_id = ctx.resolve(SELF_ID)
        reply_to = None
        if "post_type" in raw:
            protocol = "onebot"
            account = str(self_id)
            values = raw.get("message", [])
            # CQ encoded strings cannot be treated as literal user text.
            values = [{"type": "text", "data": {"text": values}}] if isinstance(values, str) and "[CQ:" not in values else values
            if not isinstance(values, list):
                values = [{"type": "unsupported"}]
            parts = []
            for segment in values:
                if isinstance(segment, dict) and segment.get("type") == "text":
                    data = segment.get("data", {})
                    if isinstance(data, dict) and isinstance(data.get("text"), str):
                        parts.append(data["text"])
                else:
                    parts.append(" ")
            plain = "".join(parts)
            sender = raw.get("sender") if isinstance(raw.get("sender"), dict) else {}
            sender_bot = str(author) == str(self_id) or bool(sender.get("is_bot") or raw.get("sender_is_bot"))
            group_admin = sender.get("role") in ("owner", "admin")
            safe = all(isinstance(s, dict) and s.get("type") in ("text", "at") for s in values)
            mentions = [s for s in values if isinstance(s, dict) and s.get("type") == "at"]
            addressed = not mentions or any(isinstance(s.get("data"), dict)
                                           and str(s["data"].get("qq")) == str(self_id) for s in mentions)
            references = [s.get("data", {}).get("id") for s in values if isinstance(s, dict) and s.get("type") == "reply" and isinstance(s.get("data"), dict)]
            if len(references) == 1 and references[0] not in (None, ""):
                reply_to = str(references[0])
        elif raw.get("t") in ("GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
            protocol = "qqofficial"
            account = str(getattr(ctx.client, "app_id", self_id))
            data = raw.get("d") or {}
            original = data.get("content", "")
            if not isinstance(original, str):
                original = ""
            mentions = [match.group()[2:-1].removeprefix("!") for match in MENTION.finditer(original)]
            addressed = not mentions or str(self_id) in mentions
            plain = MENTION.sub(" ", original)
            values = [{"type": "text", "data": {"text": plain}}] if plain.strip() else []
            attachments = data.get("attachments") or []
            if not isinstance(attachments, list):
                attachments = [None]
            for item in attachments:
                if isinstance(item, dict) and str(item.get("content_type", "")).startswith("image/"):
                    url = item.get("url", "")
                    if isinstance(url, str) and url.startswith("//"):
                        url = "https:" + url
                    values.append({"type": "image", "data": {"url": url}})
                else:
                    values.append({"type": "unsupported"})
            # Quoted, rich, voice and file messages must cut the learning chain.
            reference = data.get("message_reference")
            if isinstance(reference, dict) and reference.get("message_id") not in (None, ""):
                reply_to = str(reference["message_id"])
                values.append({"type": "reply", "data": {"id": reply_to}})
            elif reference:
                values.append({"type": "unsupported"})
            if data.get("embeds"):
                values.append({"type": "unsupported"})
            sender = data.get("author") if isinstance(data.get("author"), dict) else {}
            sender_bot = str(author) == str(self_id) or bool(sender.get("bot"))
            group_admin = False
            safe = all(s["type"] == "text" for s in values)
        else:
            return None
        if account in ("", "None"):
            return None
        namespace = json.dumps([ctx.envelope.adapter_id, account], separators=(",", ":"))
        key = json.dumps([ctx.envelope.adapter_id, account, str(target)], separators=(",", ":"))
        native_id = ctx.resolve(MESSAGE_ID)
        event_id = str(native_id) if native_id not in (None, "") else "event:" + ctx.envelope.event_id
        event_key = json.dumps([key, event_id], separators=(",", ":"))
        return cls(ctx, namespace, key, target, str(author), event_key, protocol, values, plain, safe, sender_bot, group_admin,
                   str(native_id) if native_id not in (None, "") else None, reply_to, addressed)


class MediaError(ValueError):
    pass


class MediaCache:
    def __init__(self, directory, config):
        self.root = Path(directory).resolve()
        self.config = config
        self.session = None

    async def open(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.media_timeout_seconds))

    async def close(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

    def path(self, filename):
        if not re.fullmatch(r"[0-9a-f]{64}\.(?:png|gif|jpg|webp)", filename):
            raise MediaError("invalid media reference")
        path = self.root / filename
        if not path.is_file() or path.is_symlink():
            raise MediaError("cached image is unavailable")
        return path

    async def cache(self, source):
        if not isinstance(source, str) or not source:
            raise MediaError("image has no source")
        maximum = self.config.max_image_bytes
        if source.startswith(("http://", "https://")):
            async with self.session.get(source) as response:
                response.raise_for_status()
                if response.content_length is not None and response.content_length > maximum:
                    raise MediaError("image exceeds size limit")
                data = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    data.extend(chunk)
                    if len(data) > maximum:
                        raise MediaError("image exceeds size limit")
                data = bytes(data)
        elif source.startswith("base64://"):
            encoded = source[9:]
            if len(encoded) > (maximum + 2) // 3 * 4:
                raise MediaError("image exceeds size limit")
            data = base64.b64decode(encoded, validate=True)
        else:
            # Only data from protocol adapters reaches this branch.
            parsed = urlparse(source)
            value = url2pathname(parsed.path) if parsed.scheme == "file" else source
            if parsed.scheme == "file" and parsed.netloc not in ("", "localhost"):
                value = "//" + parsed.netloc + value
            path = Path(value)
            def read():
                if not path.is_file() or path.stat().st_size > maximum:
                    raise MediaError("local image unavailable or too large")
                with path.open("rb") as stream:
                    return stream.read(maximum + 1)
            data = await asyncio.to_thread(read)
        if not data or len(data) > maximum:
            raise MediaError("invalid image size")
        suffix = ("png" if data.startswith(b"\x89PNG\r\n\x1a\n") else
                  "gif" if data.startswith((b"GIF87a", b"GIF89a")) else
                  "jpg" if data.startswith(b"\xff\xd8\xff") else
                  "webp" if data[:4] == b"RIFF" and data[8:12] == b"WEBP" else None)
        if suffix is None:
            raise MediaError("unsupported image format")
        name = hashlib.sha256(data).hexdigest() + "." + suffix
        def save():
            target = self.root / name
            if not target.exists():
                # The event loop can download identical images concurrently.
                import tempfile
                with tempfile.NamedTemporaryFile(dir=self.root, delete=False) as stream:
                    temporary = Path(stream.name)
                    stream.write(data)
                try:
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
            target.touch()
        await asyncio.to_thread(save)
        return name

    async def prune(self, references):
        def run():
            for item in self.root.iterdir():
                if re.fullmatch(r"[0-9a-f]{64}\.(?:png|gif|jpg|webp)", item.name) and item.name not in references and not item.is_symlink() and time.time() - item.stat().st_mtime > 60:
                    item.unlink(missing_ok=True)
        await asyncio.to_thread(run)


def _is_sticker(data):
    for name in ("sub_type", "subType"):
        try:
            if int(data.get(name) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass
    return (
        data.get("is_sticker") is True
        or bool(data.get("emoji_id") or data.get("emoji_package_id"))
        or any(str(data.get(name) or "").lower() in ("emoji", "sticker") for name in ("type", "_type"))
        or EXPRESSION_PLACEHOLDER.fullmatch(str(data.get("summary") or "").strip()) is not None
    )


async def parse_content(event, cache, config, settings):
    values = event.segments
    if not values or any(not isinstance(s, dict) or s.get("type") not in ("text", "at", "image", "face") for s in values):
        return None
    if any(not isinstance(s.get("data", {}), dict) for s in values):
        return None
    media_count = sum(s["type"] in ("image", "face") for s in values)
    if media_count > config.max_media_segments or media_count and not config.multimodal_enabled:
        return None
    raw_text = event.plain
    if media_count:
        raw_text = ANNOTATION.sub("", raw_text)
    if CONTROL.search(raw_text):
        return None
    text = " ".join(raw_text.split())
    if URL.search(text) or "@" in text or "＠" in text or text.startswith(config.command_prefix):
        return None
    if not (0 if media_count else config.min_text_length) <= len(text) <= config.max_text_length:
        return None
    if not media_count:
        content = Content.text(text) if text else None
        return content if content and settings.allows(content) else None
    result = []
    placeholder = EXPRESSION_PLACEHOLDER.search(event.plain) is not None
    try:
        for value in values:
            kind, data = value["type"], value.get("data") or {}
            if kind == "at":
                continue
            if kind == "text":
                part = " ".join(ANNOTATION.sub("", str(data.get("text", ""))).split())
                if part:
                    result.append(Segment("text", part))
            elif kind == "face":
                result.append(Segment("face", int(data["id"])))
            else:
                sticker = _is_sticker(data) or placeholder and media_count == 1
                if sticker and not settings.expression_enabled or not settings.expression_enabled and not settings.media_enabled:
                    return None
                source = data.get("url") or data.get("file")
                name = await cache.cache(source)
                result.append(Segment("image", name, sticker or name.endswith(".gif")))
    except (MediaError, OSError, TypeError, ValueError, KeyError, aiohttp.ClientError, TimeoutError):
        return None
    content = Content(tuple(result)) if result else None
    return content if content and settings.allows(content) else None
