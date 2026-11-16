"""Use Tiffany Client methods and keep receipts for this service's sends only."""
import asyncio
import base64
import logging
import time
from dataclasses import dataclass

from .models import Content, DeliveryReceipt

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Route:
    adapter: str
    namespace: str
    target: str | int
    protocol: str


class DeliveryError(RuntimeError):
    def __init__(self, receipts):
        super().__init__("message delivery failed")
        self.receipts = receipts


class Sender:
    def __init__(self, cache):
        self.cache = cache
        self.clients = {}
        self.routes = {}
        self.receipts = {}
        self.delivered = {}

    def bind(self, event):
        self.clients[event.namespace] = event.ctx.client
        self.routes[event.group_key] = Route(event.ctx.envelope.adapter_id, event.namespace, event.target, event.protocol)

    def client(self, key):
        route = self.routes[key]
        client = self.clients.get(route.namespace)
        if client is None or getattr(client, "running", True) is False:
            raise RuntimeError("no active client for this account")
        return route, client

    def _remember(self, key, message_id):
        self.delivered[key] = self.delivered.get(key, 0) + 1
        if message_id in (None, ""):
            logger.warning("复读发送缺少消息 ID，无法撤回：%s", key)
            return
        now = time.monotonic()
        self.receipts[key] = [(mid, stamp) for mid, stamp in self.receipts.get(key, ()) if now - stamp <= 120]
        self.receipts[key].append((message_id, now))

    async def send(self, key, content, ctx=None, *, on_receipt=None):
        receipts = []
        async def remember(message_id, sent_content):
            self._remember(key, message_id)
            receipt = DeliveryReceipt(str(message_id) if message_id not in (None, "") else None, sent_content, time.time())
            receipts.append(receipt)
            if on_receipt is not None:
                await on_receipt(receipt)
        try:
            route, client = self.client(key)
            if route.protocol == "onebot":
                segments = []
                for segment in content.segments:
                    if segment.kind == "text":
                        segments.append({"type": "text", "data": {"text": segment.value}})
                    elif segment.kind == "face":
                        segments.append({"type": "face", "data": {"id": str(segment.value)}})
                    else:
                        path = self.cache.path(str(segment.value))
                        encoded = await asyncio.to_thread(lambda path=path: base64.b64encode(path.read_bytes()).decode("ascii"))
                        segments.append({"type": "image", "data": {"file": "base64://" + encoded}})
                result = await client.call("send_group_msg", {"group_id": route.target, "message": segments})
                message_id = result.data.get("message_id") if isinstance(result.data, dict) else None
                await remember(message_id, content)
            else:
                if any(s.kind == "face" for s in content.segments):
                    raise ValueError("QQ official does not support OneBot face segments")
                async def deliver(payload, sent_content):
                    # An incoming reply uses that event's sequence counter. Timer
                    # sends deliberately have no expired passive message reference.
                    response = await client.reply_message(ctx, json=payload) if ctx is not None else await client.send_message("group", str(route.target), json=payload)
                    message_id = response.get("id") if isinstance(response, dict) else None
                    await remember(message_id, sent_content)
                images = [s for s in content.segments if s.kind == "image"]
                if not images:
                    await deliver({"msg_type": 0, "content": content.plain}, content)
                for index, image in enumerate(images):
                    uploaded = await client.upload_file("group", str(route.target), self.cache.path(str(image.value)), file_type=1)
                    info = uploaded.get("file_info") if isinstance(uploaded, dict) else None
                    if not isinstance(info, str) or not info:
                        raise ValueError("QQ upload returned no file_info")
                    payload = {"msg_type": 7, "media": {"file_info": info}}
                    if index == 0 and content.plain:
                        payload["content"] = content.plain
                    from .models import Segment
                    sent_content = Content((Segment("text", content.plain), image)) if index == 0 and content.plain else Content((image,))
                    await deliver(payload, sent_content)
        except Exception as exc:
            raise DeliveryError(receipts) from exc
        return receipts

    async def recall(self, key):
        route, client = self.client(key)
        now = time.monotonic()
        history = [(mid, stamp) for mid, stamp in self.receipts.get(key, ()) if now - stamp <= 120]
        self.receipts[key] = history
        if not history:
            return False
        target = history[-1]
        if route.protocol == "onebot":
            await client.call("delete_msg", {"message_id": target[0]})
        else:
            await client.delete_message("group", str(route.target), str(target[0]))
        history.remove(target)
        return True

    async def notice(self, key, text):
        route, client = self.client(key)
        if route.protocol == "onebot":
            await client.call("send_group_msg", {"group_id": route.target, "message": [{"type": "text", "data": {"text": text}}]})
        else:
            await client.send_message("group", str(route.target), json={"msg_type": 0, "content": text})
