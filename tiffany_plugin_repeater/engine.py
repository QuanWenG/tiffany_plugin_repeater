"""Learning policy independent of event dispatch and transport."""
import json
import logging
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace

from .context import (
    CHANNEL_WEIGHTS,
    MAX_CANDIDATES,
    POLICY,
    contextual_distribution,
    normalize_weights,
)
from .models import Content, ReplyDecision

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Activity:
    count: int = 0
    first: float = 0
    last: float = 0
    spoke: float | None = None
    words_sent: deque = field(default_factory=lambda: deque(maxlen=100))


class Engine:
    def __init__(self, store, config, rng=None):
        self.store, self.config = store, config
        self.rng = rng or random.Random()
        self.activity = {}
        self.drunk_until = {}
        self.shadow_rng = random.Random(0)
        self.selection_times = deque(maxlen=4096)
        self.cache = None

    def remaining(self, key, now=None):
        return max(0, self.drunk_until.get(key, 0) - (time.time() if now is None else now))

    def effective(self, key, settings, now=None):
        return self.config.effective(settings.preset, bool(self.remaining(key, now)))

    async def learned(self, event, content, now):
        await self.store.learn(event.group_key, content, event.author, event.event_key, now)
        activity = self.activity.setdefault(event.group_key, Activity(first=now))
        activity.count += 1
        activity.last = now

    def sent(self, key, content, now):
        activity = self.activity.get(key)
        if activity:
            activity.spoke = now
            activity.words_sent.append(content.words)

    async def allowed(self, key, content):
        settings = await self.store.settings(key)
        return self.config.enabled and settings.allows(content) and (self.config.multimodal_enabled or not content.media) and not await self.store.banned(key, content)

    def _static_allowed(self, bundle, content):
        if "content_bans" not in bundle:
            bundle["content_bans"] = {r["value"] for r in bundle["ban"] if r["kind"] == "content"}
            bundle["keyword_bans"] = {r["value"] for r in bundle["ban"] if r["kind"] == "keyword"}
        return (self.config.enabled and bundle["settings"].allows(content)
                and (self.config.multimodal_enabled or not content.media)
                and content.key not in bundle["content_bans"] and content.words not in bundle["keyword_bans"])

    async def _legacy_buckets(self, event, prompt, *, force=False, bundle=None):
        key = event.group_key
        bundle = bundle or await self.store.selection_bundle(key, max(self.config.duplicate_window, self.config.topics_size))
        settings = bundle["settings"]
        config = self.effective(key, settings)
        rows = await self.store.candidates(event.namespace, prompt.words)
        buckets = defaultdict(list)
        for row in rows:
            buckets[row["signature"], bool(row["media"])].append(row)
        recent = bundle["recent"][:max(config.duplicate_window, config.topics_size)]
        recent = [r for r in recent if r["author"] not in config.ignored_user_ids]
        sent = bundle["sent"][:config.duplicate_window]
        recent_keys = {r["content_key"] for r in recent[:config.duplicate_window]}
        sent_keys = {r["content_key"] for r in sent}
        sent_words = {r["words"] for r in sent}
        topic_tokens = " ".join(r["words"] for r in recent[:config.topics_size]).split()[:config.topics_size]
        low = config.answer_threshold - len(config.answer_weights) + 1
        threshold = 1 if force else self.rng.choices(range(low, config.answer_threshold + 1), weights=config.answer_weights)[0]
        if len(prompt.words.split()) >= 2:
            threshold = max(1, threshold - 1)
        pool, weights, origins = [], [], {}
        for (_, media), variants in buckets.items():
            eligible = [r for r in variants if not media or r["group_key"] == key]
            groups = {r["group_key"] for r in eligible}
            threshold_groups = 2 if prompt.addressed else config.cross_group_threshold
            if not eligible or key not in groups and len(groups) < threshold_groups:
                continue
            counts = {r["group_key"]: r["relation_count"] for r in eligible}
            learned_count = sum(counts.values())
            if learned_count < threshold:
                continue
            merged = {}
            for row in eligible:
                content = Content.load(row["payload"]).answer(addressed=prompt.addressed or force)
                if content is None or not self._static_allowed(bundle, content):
                    continue
                if force:
                    if content.media or not content.plain:
                        continue
                else:
                    if content.key == prompt.key or content.key in sent_keys or row["content_key"] in sent_keys or content.words == prompt.words or content.words in sent_words:
                        continue
                    if prompt.media and not content.media:
                        continue
                    if not self.remaining(key) and learned_count < 3 and (content.key in recent_keys or row["content_key"] in recent_keys):
                        continue
                previous = merged.get(content.key)
                count = row["count"] + (previous[1] if previous else 0)
                merged[content.key] = (content, count)
                origins[content.key] = row["content_key"]
            if merged:
                contents, variant_weights = zip(*merged.values())
                pool.append((contents, variant_weights))
                topical = sum(topic_tokens.count(w) for w in eligible[0]["words"].split())
                weights.append(min(learned_count, 10) + 10000 * topical)
        return pool, weights, origins, threshold

    async def _answer(self, event, prompt, *, force=False):
        pool, weights, _, _ = await self._legacy_buckets(event, prompt, force=force)
        if not pool:
            return None
        contents, variant_weights = self.rng.choices(pool, weights=weights)[0]
        return self.rng.choices(contents, weights=variant_weights)[0]

    @staticmethod
    def _legacy_distribution(pool, weights):
        result, contents = {}, {}
        total = sum(weights)
        for (variants, variant_weights), weight in zip(pool, weights):
            count = sum(variant_weights)
            for content, frequency in zip(variants, variant_weights):
                result[content.key] = result.get(content.key, 0) + weight / total * frequency / count
                contents[content.key] = content
        return result, contents

    def _bound_legacy(self, pool, weights):
        if sum(len(variants) for variants, _ in pool) <= MAX_CANDIDATES:
            return pool, weights
        distribution, _ = self._legacy_distribution(pool, weights)
        selected = set(sorted(distribution, key=lambda key: (-distribution[key], key))[:MAX_CANDIDATES])
        bounded, adjusted = [], []
        for (variants, frequencies), weight in zip(pool, weights):
            kept = [(c, n) for c, n in zip(variants, frequencies) if c.key in selected]
            if kept:
                values, counts = zip(*kept)
                bounded.append((values, counts))
                adjusted.append(weight * sum(counts) / sum(frequencies))
        return bounded, adjusted

    async def choose(self, event, content):
        decision = await self.decide(event, content)
        return decision.content if decision and decision.mode != "shadow" else None

    async def decide(self, event, content, history=(), *, mode=None):
        started = time.perf_counter()
        try:
            return await self._decide(event, content, history, mode=mode)
        finally:
            self.selection_times.append(time.perf_counter() - started)

    async def _decide(self, event, content, history, *, mode=None):
        key = event.group_key
        bundle = await self.store.selection_bundle(key, max(self.config.repeat_threshold, self.config.duplicate_window, self.config.topics_size))
        if not self._static_allowed(bundle, content):
            return None
        settings = bundle["settings"]
        config = self.effective(key, settings)
        mode = mode or settings.intelligence_mode or config.intelligence_mode
        decision = ReplyDecision(content, content.key, content.dump(), getattr(event, "message_id", None) or event.event_key,
                                 tuple(history), content.key, data_version=bundle["revision"])
        recent = bundle["recent"][:max(config.repeat_threshold - 1, config.duplicate_window)]
        recent = [r for r in recent if r["author"] not in config.ignored_user_ids]
        tail = config.repeat_threshold - 1
        if not content.addressed and len(recent) >= tail and all(r["content_key"] == content.key for r in recent[:tail]):
            sent = bundle["sent"][:1]
            return None if sent and sent[0]["content_key"] == content.key else replace(decision, mode="repeat", source="repeat")
        if not content.plain:
            return None
        if not content.addressed and not self.remaining(key) and len(content.words.split()) <= 1 and len(content.plain) <= 2:
            return None
        pool, weights, origins, threshold = await self._legacy_buckets(event, content, bundle=bundle)
        if mode == "auto":
            pool, weights = self._bound_legacy(pool, weights)
        legacy, contents = self._legacy_distribution(pool, weights)
        channels = {"legacy": legacy}
        metadata = {k: {"raw_key": origins[k], "support": None, "provenance": {"legacy": {
            "raw_key": origins[k], "source_expression": content.words, "evidence_refs": []}}} for k in legacy}
        feedback = {}
        if mode != "off" and not content.media:
            try:
                extra = await self.store.intelligence_bundle(key, content, history, threshold=threshold)
                channels.update(self._new_channels(content, history, bundle, extra, threshold, contents, metadata))
                feedback = extra["feedback"]
            except Exception:
                logger.exception("智能候选检索失败，回退原检索")
                self.store.metrics["retrieval_failures"] += 1
        smart = any(values for name, values in channels.items() if name != "legacy") or bool(feedback)
        if mode == "off" or not smart:
            if not pool:
                return None
            variants, frequencies = self.rng.choices(pool, weights=weights)[0]
            chosen = self.rng.choices(variants, weights=frequencies)[0]
            return replace(decision, content=chosen, raw_answer_key=origins[chosen.key], probability=legacy[chosen.key])
        base, contributions = {}, {}
        for source, distribution in channels.items():
            for answer, probability in normalize_weights(distribution).items():
                value = CHANNEL_WEIGHTS[source] * probability
                base[answer] = base.get(answer, 0) + value
                contributions.setdefault(answer, {})[source] = value
        selected = sorted(base, key=lambda k: (-base[k], k))[:MAX_CANDIDATES]
        base = normalize_weights({k: base[k] for k in selected})
        amplified = normalize_weights({k: p * feedback.get(k, 1) for k, p in base.items()})
        final = {k: (1 - POLICY.exploration) * amplified[k] + POLICY.exploration * base[k] for k in base}
        diagnostics = tuple({"answer_key": k, "text": contents[k].plain, "base_probability": base[k],
                             "feedback_multiplier": feedback.get(k, 1), "probability": final[k],
                             "sources": contributions[k], **metadata[k]}
                            for k in sorted(final, key=lambda k: (-final[k], k))[:10])
        if mode == "observe":
            shadow_key = self.shadow_rng.choices(list(final), weights=list(final.values()))[0]
            if not pool:
                self.store.metrics["shadow_only_candidates"] += 1
                return replace(decision, content=contents[shadow_key], mode="shadow", source="shadow", probability=0,
                               raw_answer_key=metadata[shadow_key]["raw_key"], diagnostics=diagnostics,
                               shadow={"answer_key": shadow_key, "probability": final[shadow_key]})
            variants, frequencies = self.rng.choices(pool, weights=weights)[0]
            chosen = self.rng.choices(variants, weights=frequencies)[0]
            return replace(decision, content=chosen, raw_answer_key=origins[chosen.key], probability=legacy[chosen.key],
                           diagnostics=diagnostics, shadow={"answer_key": shadow_key, "probability": final[shadow_key]})
        chosen_key = self.rng.choices(list(final), weights=list(final.values()))[0]
        source = max(contributions[chosen_key], key=contributions[chosen_key].get)
        provenance = metadata[chosen_key]["provenance"][source]
        self.store.metrics["selected_" + source] += 1
        return replace(decision, content=contents[chosen_key], raw_answer_key=provenance["raw_key"],
                       source=source, probability=final[chosen_key], diagnostics=diagnostics,
                       source_expression=provenance["source_expression"], evidence_refs=tuple(provenance["evidence_refs"]))

    def _new_channels(self, prompt, history, bundle, extra, threshold, contents, metadata):
        channels = {"direct": {}, "graph": {}, "experience": {}}
        rows = extra["rows"]
        direct = contextual_distribution([r for r in rows if r["prompt"] in extra["anchors"]], extra["query"], extra["texts"], extra["neighbors"], threshold)
        graph = {}
        for source, similarity in extra["graph_sources"].items():
            # Borrow only observed one-step answers; keep the strongest path per answer.
            for row in rows:
                if row["prompt"] == source and row["depth"] == 1 and row["independent_count"] >= max(POLICY.direct_support, threshold):
                    graph[row["answer"]] = max(graph.get(row["answer"], 0), similarity * row["effective_count"])
        # utterances carry content, while support lives in the aggregate rows.
        candidates = {k: (r["payload"], 3) for k, r in extra["utterances"].items()}
        experience = {r["answer_key"]: r["successes"] for r in extra["experience"]}
        for row in extra["experience"]:
            candidates[row["answer_key"]] = (row["payload"], row["successes"])
        sent_keys = {r["content_key"] for r in bundle["sent"][:self.config.duplicate_window]}
        sent_words = {r["words"] for r in bundle["sent"][:self.config.duplicate_window]}
        for source, distribution in (("direct", direct), ("graph", graph), ("experience", experience)):
            for raw_key, weight in distribution.items():
                if raw_key not in candidates:
                    continue
                candidate = Content.load(candidates[raw_key][0]).answer(addressed=prompt.addressed)
                if candidate is None or not self._static_allowed(bundle, candidate):
                    continue
                if self.cache is not None:
                    try:
                        for segment in candidate.segments:
                            if segment.kind == "image":
                                self.cache.path(str(segment.value))
                    except (ValueError, OSError):
                        continue
                if candidate.key == prompt.key or candidate.words == prompt.words or candidate.key in sent_keys or raw_key in sent_keys or candidate.words in sent_words:
                    continue
                contents[candidate.key] = candidate
                channels[source][candidate.key] = channels[source].get(candidate.key, 0) + weight
                relevant = [r for r in rows if r["answer"] == raw_key and r["prompt"] in (extra["graph_sources"] if source == "graph" else extra["anchors"])]
                if source == "graph" and relevant:
                    strongest = max(relevant, key=lambda r: extra["graph_sources"][r["prompt"]] * r["effective_count"])
                    relevant = [r for r in relevant if r["prompt"] == strongest["prompt"] and r["depth"] == 1]
                provenance = {"raw_key": raw_key, "source_expression": relevant[0]["prompt"] if relevant else prompt.key,
                              "evidence_refs": [{"type": "human", "context": r["context"], "answer_key": raw_key} for r in relevant[:3]]}
                if source == "experience":
                    provenance["evidence_refs"] = [{"type": "robot", "episode_ids": json.loads(r["episode_ids"])[:10]}
                                                    for r in extra["experience"] if r["answer_key"] == raw_key]
                entry = metadata.setdefault(candidate.key, {"raw_key": raw_key, "support": 0, "provenance": {}})
                entry["support"] = max(entry["support"] or 0, max((r["independent_count"] for r in relevant), default=0))
                entry["provenance"][source] = provenance
        return channels

    async def forced(self, event):
        for row in await self.store.recent(event.group_key, self.config.topics_size):
            if row["author"] in self.config.ignored_user_ids:
                continue
            prompt = Content.load(row["payload"])
            if prompt.plain:
                content = await self._answer(event, prompt, force=True)
                if content:
                    return content
        return None

    async def proactive(self, key, now):
        activity = self.activity.get(key)
        if activity is None or activity.count < 10 or activity.spoke is None or activity.last < activity.spoke:
            return None
        settings = await self.store.settings(key)
        config = self.effective(key, settings, now)
        average = (activity.last - activity.first) / activity.count
        if now - activity.last < config.proactive_delay_seconds + average * config.proactive_activity_factor:
            return None
        sent = await self.store.recent(key, config.duplicate_window, sent=True)
        excluded = {r["content_key"] for r in sent}
        pool = []
        for row in await self.store.recent(key, config.proactive_recent_messages):
            if row["author"] in config.ignored_user_ids:
                continue
            content = Content.load(row["payload"])
            if content.key not in excluded and content.answer() is not None and await self.allowed(key, content):
                pool.append(content)
        return self.rng.choice(pool) if pool else None
