"""One-hop shared-reply similarity, computed off the runtime event loop."""
import asyncio
import logging
import math
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from .context import GENERIC, GENERIC_TEXTS, POLICY, normalize

logger = logging.getLogger(__name__)


def build_neighbors(snapshot, budget=POLICY.graph_budget):
    started = time.perf_counter()
    edges = snapshot["edges"]
    count = len({r["prompt"] for r in edges})
    if count < POLICY.graph_minimum_nodes:
        return {}
    inverse = defaultdict(set)
    for row in edges:
        inverse[row["answer"]].add(row["prompt"])
    vectors = defaultdict(dict)
    for row in edges:
        degree = len(inverse[row["answer"]])
        if degree >= max(POLICY.generic_degree_floor, math.ceil(POLICY.generic_degree_fraction * count)) or degree > POLICY.reverse_limit or row["independent_count"] < POLICY.graph_shared_support:
            continue
        text = normalize(row["answer_text"])
        if not row["answer_media"] and (GENERIC.fullmatch(row["answer_text"].strip()) or GENERIC.fullmatch(text) or text in GENERIC_TEXTS):
            continue
        weight = math.log1p(row["effective_count"]) * math.log((count + 1) / (degree + 1))
        if weight > 0:
            vectors[row["prompt"]][row["answer"]] = weight
    eligible = defaultdict(set)
    for node, vector in vectors.items():
        for answer in vector:
            eligible[answer].add(node)
    result = {}
    for dirty in snapshot["dirty"]:
        node = dirty["node"]
        vector = vectors.get(node, {})
        overlap = defaultdict(int)
        for answer in vector:
            for other in eligible[answer]:
                if other != node:
                    overlap[other] += 1
        candidates = sorted((other for other in overlap if overlap[other] >= POLICY.graph_shared_answers), key=lambda k: (-overlap[k], k))[:POLICY.graph_comparisons]
        neighbors = []
        for other in candidates:
            right = vectors[other]
            union = vector.keys() | right.keys()
            denominator = sum(max(vector.get(k, 0), right.get(k, 0)) for k in union)
            score = sum(min(vector.get(k, 0), right.get(k, 0)) for k in union) / denominator if denominator else 0
            if score >= POLICY.graph_similarity:
                neighbors.append((other, score, sorted(vector.keys() & right.keys())))
        result[node] = sorted(neighbors, key=lambda n: (-n[1], n[0]))[:POLICY.graph_neighbors]
        if time.perf_counter() - started >= budget:
            break
    return result


class GraphWorker:
    def __init__(self, store):
        self.store = store
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="repeater-graph")
        self.pending = set()

    def _settle(self, future):
        self.pending.discard(future)
        if not future.cancelled():
            future.exception()

    async def tick(self):
        started, processed = time.perf_counter(), 0
        while processed < POLICY.graph_batch and time.perf_counter() - started < POLICY.graph_budget:
            snapshot = await self.store.graph_snapshot(POLICY.graph_batch - processed)
            if snapshot is None:
                break
            future = asyncio.wrap_future(self.executor.submit(build_neighbors, snapshot, max(.001, POLICY.graph_budget - (time.perf_counter() - started))))
            self.pending.add(future)
            future.add_done_callback(self._settle)
            results = await asyncio.shield(future)
            if await self.store.commit_neighbors(snapshot, results, time.time()):
                self.store.metrics["graph_nodes_updated"] += len(results)
            processed += max(1, len(results))

    async def close(self):
        if self.pending:
            await asyncio.shield(asyncio.gather(*tuple(self.pending), return_exceptions=True))
        self.executor.shutdown(wait=False)
