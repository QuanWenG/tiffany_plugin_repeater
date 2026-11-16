"""Versioned, deterministic matching rules. Content identities remain unchanged."""
import json
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True, slots=True)
class Policy:
    """Internal versioned rules; these are not additional public options."""
    version: int = 1
    segment_seconds: int = 120
    independence_seconds: int = 300
    direct_support: int = 3
    effective_support: float = 1.5
    context_support: tuple = (0, 3, 3, 5)
    context_prior: float = 5
    lexical_minimum: float = .6
    anchors: int = 5
    graph_minimum_nodes: int = 20
    graph_shared_support: int = 2
    graph_shared_answers: int = 2
    graph_similarity: float = .35
    generic_degree_floor: int = 10
    generic_degree_fraction: float = .2
    reverse_limit: int = 200
    graph_comparisons: int = 200
    graph_neighbors: int = 20
    graph_batch: int = 20
    graph_budget: float = .25
    candidates: int = 200
    rewards: tuple = (0., .5, .8, 1.)
    feedback_exposures: int = 3
    feedback_positives: int = 2
    feedback_gain: float = 2
    feedback_prior: float = 8
    exploration: float = .1
    experience_episodes: int = 2
    experience_people: int = 2
    channels: tuple = (("direct", .55), ("legacy", .25), ("graph", .15), ("experience", .05))


POLICY = Policy()
POLICY_VERSION = POLICY.version
SEGMENT_SECONDS = POLICY.segment_seconds
INDEPENDENCE_SECONDS = POLICY.independence_seconds
MAX_CANDIDATES = POLICY.candidates
CHANNEL_WEIGHTS = dict(POLICY.channels)
NEGATION = re.compile(r"不|没|无|别|勿|未|莫|非|甭")
NUMBERS = re.compile(r"\d+|[零〇一二两三四五六七八九十百千万亿]+")
GENERIC = re.compile(r"(?:[哈呵嘿嘻]+|笑死(?:我了)?|[hHaA]+|[6]+|[?？!！]+)")
GENERIC_TEXTS = frozenset(("笑死", "笑死我了", "乐", "草", "艹", "好", "好的", "好呀", "哦", "嗯", "嗯嗯", "是的", "确实"))


def packed(values):
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


@lru_cache(maxsize=16384)
def normalize(text):
    text = unicodedata.normalize("NFKC", text).casefold()
    return "".join(c for c in text if not c.isspace() and not unicodedata.category(c).startswith("P"))


@lru_cache(maxsize=16384)
def grams(text):
    text = normalize(text)
    return frozenset(text[i:i + 2] for i in range(len(text) - 1)) or frozenset([text])


def lexical_similarity(a, b):
    a, b = normalize(a), normalize(b)
    if a == b:
        return 1.0
    if NUMBERS.findall(a) != NUMBERS.findall(b) or NEGATION.findall(a) != NEGATION.findall(b):
        return 0.0
    left, right = grams(a), grams(b)
    value = len(left & right) / max(1, len(left | right))
    return value if value >= POLICY.lexical_minimum else 0.0


def sequence_similarity(expected, actual, texts, neighbors):
    if len(expected) != len(actual):
        return 0.0
    result = 1.0
    for left, right in zip(expected, actual):
        if left == right:
            continue
        lexical = lexical_similarity(texts[left], texts[right]) if texts.get(left) and texts.get(right) else 0
        graph = max(neighbors.get((left, right), 0), neighbors.get((right, left), 0))
        result *= max(lexical, graph if graph >= POLICY.graph_similarity else 0)
        if not result:
            break
    return result


def normalize_weights(weights):
    total = sum(weights.values())
    return {key: value / total for key, value in weights.items() if value > 0} if total else {}


def contextual_distribution(rows, query, texts, neighbors, threshold=POLICY.direct_support):
    """Each order is a separate view; interpolate distributions, never add counts."""
    orders = {1: {}, 2: {}, 3: {}}
    supports = {1: 0., 2: 0., 3: 0.}
    for row in rows:
        depth = row["depth"]
        minimum = POLICY.context_support[depth]
        if row["independent_count"] < max(minimum, threshold) or row["effective_count"] < POLICY.effective_support or len(query) < depth:
            continue
        context = json.loads(row["context"])
        match = sequence_similarity(context, query[-depth:], texts, neighbors)
        if not match:
            continue
        value = row["effective_count"] * match
        orders[depth][row["answer"]] = orders[depth].get(row["answer"], 0) + value
        supports[depth] += value
    distribution = normalize_weights(orders[1])
    for depth in (2, 3):
        if not orders[depth]:
            continue
        longer = normalize_weights(orders[depth])
        factor = supports[depth] / (supports[depth] + POLICY.context_prior)
        distribution = {key: factor * longer.get(key, 0) + (1 - factor) * distribution.get(key, 0)
                        for key in distribution.keys() | longer.keys()}
    return normalize_weights(distribution)
