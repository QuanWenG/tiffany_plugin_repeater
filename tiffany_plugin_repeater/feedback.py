"""Protocol feedback normalization and short-lived out-of-order attribution."""
import json
import time
from collections import OrderedDict
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeedbackObservation:
    group_key: str
    target_id: str
    actor: str
    kind: str
    event_key: str
    active: bool
    created: float


def connection_key(client):
    return str(getattr(client, "connection_id", None) or "client:" + str(id(client)))


def reaction_observations(ctx, now=None):
    raw = ctx.raw
    if raw.get("post_type") != "notice" or raw.get("notice_type") != "group_msg_emoji_like":
        return []
    account, group, target = raw.get("self_id"), raw.get("group_id"), raw.get("message_id")
    if any(v in (None, "") for v in (account, group, target)) or type(raw.get("is_add")) is not bool:
        return []
    key = json.dumps([ctx.envelope.adapter_id, str(account), str(group)], separators=(",", ":"))
    actor = str(raw["user_id"]) if raw.get("user_id") not in (None, "", 0, "0") else ""
    if actor == str(account):
        return []
    likes = raw.get("likes")
    if not isinstance(likes, list):
        return []
    stamp = time.time() if now is None else now
    return [FeedbackObservation(key, str(target), actor, "reaction", str(item["emoji_id"]), raw["is_add"], stamp)
            for item in likes if isinstance(item, dict) and item.get("emoji_id") not in (None, "")][:32]


class FeedbackCollector:
    def __init__(self, store):
        self.store = store
        self.pending = OrderedDict()
        self.lost_groups = set()

    async def capture(self, observation):
        args = (observation.group_key, observation.target_id, observation.actor, observation.kind,
                observation.event_key, observation.active, observation.created)
        try:
            if await self.store.record_feedback(*args):
                return True
        except Exception:
            self.store.metrics["feedback_capture_failures"] += 1
            self.lost_groups.add(observation.group_key)
            await self.store.invalidate_observations(key=observation.group_key)
            raise
        key = args[:5]
        self.pending[key] = observation
        self.pending.move_to_end(key)
        if len(self.pending) > 1000:
            self.pending.popitem(last=False)
            self.store.metrics["feedback_buffer_overflow"] += 1
        return False

    async def flush(self, now=None, *, group_key=None, target_id=None):
        now = time.time() if now is None else now
        for key in tuple(self.lost_groups):
            await self.store.invalidate_observations(key=key)
            self.lost_groups.discard(key)
        for key, observation in tuple(self.pending.items()):
            if now - observation.created > 60:
                self.pending.pop(key, None)
                self.store.metrics["feedback_buffer_expired"] += 1
                continue
            if group_key is not None and (key[0] != group_key or key[1] != target_id):
                continue
            if await self.store.record_feedback(observation.group_key, observation.target_id, observation.actor,
                                                observation.kind, observation.event_key, observation.active, observation.created):
                self.pending.pop(key, None)

    def discard_group(self, key):
        self.lost_groups.discard(key)
        for identity in list(self.pending):
            if identity[0] == key:
                self.pending.pop(identity, None)
