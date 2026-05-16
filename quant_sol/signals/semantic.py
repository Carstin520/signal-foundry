from __future__ import annotations

import math
import os
import json
from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Sequence

import duckdb
import requests
from requests import RequestException

from .config import SemanticMatchingConfig
from .history import case_keywords, direction_from_text, get_event_case, matched_keywords
from .storage import upsert_event_case_posts, upsert_post_market_semantic_matches
from .utils import stable_hash


class SemanticMatcherUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class SemanticMatchResult:
    matches_written: int
    posts_added: int
    unavailable_reason: Optional[str] = None


def match_event_posts_semantically(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    config: SemanticMatchingConfig,
    method: str = "semantic",
) -> SemanticMatchResult:
    case = get_event_case(con, case_id)
    if not case or not case.get("market_slug"):
        return SemanticMatchResult(0, 0, "case_not_found_or_unresolved")
    try:
        encoder = _SentenceTransformerEncoder(config.model_name)
    except Exception as exc:
        return SemanticMatchResult(0, 0, f"semantic_model_unavailable: {exc}")

    posts = _candidate_posts(con, case_id, config.max_posts)
    concepts = _case_concepts(case_id, case, config)
    excludes = _case_excludes(case_id, config)
    if not concepts or not posts:
        return SemanticMatchResult(0, 0, None)

    concept_vectors = encoder.encode(concepts)
    exclude_vectors = encoder.encode(excludes) if excludes else []
    rows = []
    post_rows = []
    for post in posts:
        text = str(post.get("text") or "")
        if not text:
            continue
        vector = encoder.encode([text])[0]
        best_index, similarity = _best_similarity(vector, concept_vectors)
        reject_similarity = max((_cosine(vector, item) for item in exclude_vectors), default=0.0)
        if similarity < config.similarity_threshold or reject_similarity >= similarity:
            continue
        matched_concepts = [concepts[best_index]]
        rows.append(
            {
                "case_id": case_id,
                "post_id": post["post_id"],
                "handle": post["handle"],
                "market_slug": case["market_slug"],
                "method": method,
                "similarity": similarity,
                "matched_concepts": matched_concepts,
                "rejected_concepts": excludes if reject_similarity >= config.similarity_threshold else [],
                "decision": "matched",
            }
        )
        keywords = matched_keywords(text, case.get("keywords") or case_keywords(str(case.get("query") or "")))
        semantic_keywords = [f"semantic:{concept}" for concept in matched_concepts]
        post_rows.append(
            {
                "case_id": case_id,
                "post_id": post["post_id"],
                "handle": post["handle"],
                "created_at": post["created_at"],
                "text": text,
                "direction": direction_from_text(text),
                "matched_keywords": list(dict.fromkeys(keywords + semantic_keywords)),
                "raw_json": post.get("raw_json") or post,
            }
        )

    matches_written = upsert_post_market_semantic_matches(con, rows)
    posts_added = upsert_event_case_posts(con, post_rows)
    return SemanticMatchResult(matches_written, posts_added, None)


def match_event_posts_with_cloud_model(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    config: SemanticMatchingConfig,
    api_key: Optional[str] = None,
    base_url: str = "https://api.openai.com/v1",
) -> SemanticMatchResult:
    if config.cloud_provider != "openai":
        return SemanticMatchResult(0, 0, f"unsupported_cloud_provider: {config.cloud_provider}")
    key = api_key or _cloud_api_key(config.cloud_api_key_env)
    if not key:
        return SemanticMatchResult(0, 0, f"{config.cloud_api_key_env} is not set")
    case = get_event_case(con, case_id)
    if not case or not case.get("market_slug"):
        return SemanticMatchResult(0, 0, "case_not_found_or_unresolved")
    posts = _candidate_posts(con, case_id, config.max_posts)
    if not posts:
        return SemanticMatchResult(0, 0, None)

    rows = []
    post_rows = []
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    batch_size = max(1, min(config.cloud_max_posts_per_request, 50))
    concepts = _case_concepts(case_id, case, config)
    excludes = _case_excludes(case_id, config)
    for batch in _chunks(posts, batch_size):
        payload = _openai_semantic_payload(config.cloud_model, case, concepts, excludes, batch)
        try:
            response = session.post(f"{base_url.rstrip('/')}/responses", json=payload, timeout=60)
            response.raise_for_status()
        except RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            return SemanticMatchResult(matches_written=0, posts_added=0, unavailable_reason=f"cloud_model_request_failed status={status or 'unknown'}: {exc}")
        decisions = _parse_openai_decisions(response.json())
        decision_by_id = {str(item.get("post_id")): item for item in decisions}
        for post in batch:
            decision = decision_by_id.get(str(post.get("post_id")))
            if not decision or not bool(decision.get("match")):
                continue
            confidence = float(decision.get("confidence") or 0)
            if confidence < config.similarity_threshold:
                continue
            concepts_hit = [str(item) for item in decision.get("matched_concepts") or [] if str(item)]
            rows.append(
                {
                    "case_id": case_id,
                    "post_id": post["post_id"],
                    "handle": post["handle"],
                    "market_slug": case["market_slug"],
                    "method": "cloud",
                    "similarity": confidence,
                    "matched_concepts": concepts_hit,
                    "rejected_concepts": [],
                    "decision": "matched",
                }
            )
            keywords = matched_keywords(str(post.get("text") or ""), case.get("keywords") or case_keywords(str(case.get("query") or "")))
            cloud_keywords = [f"cloud:{concept}" for concept in concepts_hit] or ["cloud:semantic_match"]
            direction = str(decision.get("direction") or direction_from_text(str(post.get("text") or "")))
            if direction not in {"bullish", "bearish", "watch_only"}:
                direction = "watch_only"
            post_rows.append(
                {
                    "case_id": case_id,
                    "post_id": post["post_id"],
                    "handle": post["handle"],
                    "created_at": post["created_at"],
                    "text": post.get("text") or "",
                    "direction": direction,
                    "matched_keywords": list(dict.fromkeys(keywords + cloud_keywords)),
                    "raw_json": post.get("raw_json") or post,
                }
            )
    matches_written = upsert_post_market_semantic_matches(con, rows)
    posts_added = upsert_event_case_posts(con, post_rows)
    return SemanticMatchResult(matches_written, posts_added, None)


class _SentenceTransformerEncoder:
    def __init__(self, model_name: str) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise SemanticMatcherUnavailable("install with `pip install -e .[semantic]`") from exc
        self.model = SentenceTransformer(model_name)

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        vectors = self.model.encode(list(texts), normalize_embeddings=True)
        return [list(map(float, vector)) for vector in vectors]


def _candidate_posts(con: duckdb.DuckDBPyConnection, case_id: str, max_posts: int) -> list[dict]:
    case = get_event_case(con, case_id)
    if not case:
        return []
    rows = con.execute(
        """
        with candidates as (
            select post_id, handle, created_at, text, raw_json
            from x_posts
            where created_at between ? and ?
            union all
            select post_id, handle, created_at, text, raw_json
            from social_posts
            where platform = 'x' and created_at between ? and ?
        ),
        deduped as (
            select post_id, any_value(handle) as handle, min(created_at) as created_at,
                   any_value(text) as text, any_value(raw_json) as raw_json
            from candidates
            group by post_id
        )
        select post_id, handle, created_at, text, raw_json
        from deduped p
        where not exists (
            select 1 from event_case_posts e
            where e.case_id = ? and e.post_id = p.post_id
        )
        order by created_at
        limit ?
        """,
        [case.get("start_at"), case.get("end_at"), case.get("start_at"), case.get("end_at"), case_id, max_posts],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _case_concepts(case_id: str, case: Mapping[str, object], config: SemanticMatchingConfig) -> list[str]:
    concepts = []
    concepts.extend(config.case_seed_concepts.get(case_id) or ())
    concepts.extend(config.case_seed_concepts.get("default") or ())
    concepts.extend(str(keyword) for keyword in (case.get("keywords") or []) if keyword)
    concepts.append(str(case.get("query") or ""))
    concepts.append(str(case.get("market_slug") or "").replace("-", " "))
    return [item for item in dict.fromkeys(concepts) if item]


def _case_excludes(case_id: str, config: SemanticMatchingConfig) -> list[str]:
    concepts = []
    concepts.extend(config.case_exclude_concepts.get(case_id) or ())
    concepts.extend(config.case_exclude_concepts.get("default") or ())
    return [item for item in dict.fromkeys(concepts) if item]


def _openai_semantic_payload(
    model: str,
    case: Mapping[str, object],
    concepts: Sequence[str],
    excludes: Sequence[str],
    posts: Sequence[Mapping[str, object]],
) -> dict:
    input_payload = {
        "case": {
            "query": case.get("query"),
            "market_slug": case.get("market_slug"),
            "keywords": case.get("keywords") or [],
            "seed_concepts": list(concepts),
            "exclude_concepts": list(excludes),
        },
        "posts": [
            {
                "post_id": post.get("post_id"),
                "handle": post.get("handle"),
                "created_at": str(post.get("created_at")),
                "text": post.get("text") or "",
            }
            for post in posts
        ],
    }
    return {
        "model": model,
        "instructions": (
            "Classify whether each public X post is materially relevant to the prediction-market case. "
            "Return only structured JSON. Match indirect catalysts if they can plausibly affect market sentiment. "
            "Do not infer private information. Direction must be bullish, bearish, or watch_only."
        ),
        "input": json.dumps(input_payload, ensure_ascii=False),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "semantic_post_matches",
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "matches": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "post_id": {"type": "string"},
                                    "match": {"type": "boolean"},
                                    "confidence": {"type": "number"},
                                    "direction": {"type": "string", "enum": ["bullish", "bearish", "watch_only"]},
                                    "matched_concepts": {"type": "array", "items": {"type": "string"}},
                                },
                                "required": ["post_id", "match", "confidence", "direction", "matched_concepts"],
                            },
                        }
                    },
                    "required": ["matches"],
                },
                "strict": True,
            }
        },
        "max_output_tokens": 2000,
    }


def _parse_openai_decisions(payload: Mapping[str, object]) -> list[dict]:
    text = payload.get("output_text")
    if not text:
        chunks = []
        for item in payload.get("output") or []:
            if not isinstance(item, Mapping):
                continue
            for content in item.get("content") or []:
                if isinstance(content, Mapping) and content.get("text"):
                    chunks.append(str(content["text"]))
        text = "".join(chunks)
    if not text:
        return []
    try:
        decoded = json.loads(str(text))
    except json.JSONDecodeError:
        return []
    matches = decoded.get("matches") if isinstance(decoded, Mapping) else None
    return [item for item in matches or [] if isinstance(item, dict)]


def _chunks(rows: Sequence[Mapping[str, object]], size: int) -> list[Sequence[Mapping[str, object]]]:
    return [rows[index:index + size] for index in range(0, len(rows), size)]


def _cloud_api_key(env_or_value: str) -> Optional[str]:
    value = (env_or_value or "").strip()
    if value.startswith("sk-"):
        return value
    return os.getenv(value)


def _best_similarity(vector: Sequence[float], candidates: Sequence[Sequence[float]]) -> tuple[int, float]:
    best_index = 0
    best_score = -1.0
    for index, candidate in enumerate(candidates):
        score = _cosine(vector, candidate)
        if score > best_score:
            best_score = score
            best_index = index
    return best_index, best_score


def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


class HashingSemanticEncoder:
    """Small deterministic test double for semantic matching."""

    def encode(self, texts: Iterable[str]) -> list[list[float]]:
        vectors = []
        for text in texts:
            buckets = [0.0] * 32
            for token in str(text).lower().split():
                buckets[int(stable_hash(token)[:4], 16) % len(buckets)] += 1.0
            norm = math.sqrt(sum(value * value for value in buckets)) or 1.0
            vectors.append([value / norm for value in buckets])
        return vectors
