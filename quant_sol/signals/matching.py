from __future__ import annotations

import json
from typing import Iterable, List, Mapping, Sequence

from .config import MarketRules
from .models import EventMention
from .utils import words


def match_posts_to_markets(posts: Iterable[dict], markets: Iterable[dict], rules: MarketRules) -> List[EventMention]:
    mentions: List[EventMention] = []
    for post in posts:
        post_id = str(post.get("post_id") or "")
        post_text = str(post.get("text") or "")
        if not post_id or not post_text:
            continue
        for market in markets:
            mention = match_post_to_market(post_id, post_text, market, rules)
            if mention is not None:
                mentions.append(mention)
    return mentions


def match_post_to_market(post_id: str, post_text: str, market: Mapping[str, object], rules: MarketRules) -> EventMention | None:
    market_slug = str(market.get("market_slug") or "")
    if not market_slug:
        return None

    market_text = " ".join(
        [
            str(market.get("question") or ""),
            market_slug,
            str(market.get("event_slug") or ""),
            str(market.get("category") or ""),
            " ".join(_json_list(market.get("tags"))),
        ]
    )
    post_words = words(post_text)
    market_words = words(market_text)

    keyword_hits = sorted({keyword for keyword in rules.keywords if keyword in post_text.lower() and keyword in market_text.lower()})
    entities = sorted(_entity_hits(post_text, market_text, rules.entities))
    action_hits = sorted(_entity_hits(post_text, market_text, rules.actions))
    direct_overlap = post_words & market_words

    confidence = 0.0
    confidence += min(len(keyword_hits) * 0.12, 0.36)
    confidence += min(len(entities) * 0.18, 0.36)
    confidence += min(len(action_hits) * 0.12, 0.24)
    confidence += min(len(direct_overlap) * 0.03, 0.18)
    confidence = min(confidence, 1.0)

    if confidence < 0.25:
        return None
    return EventMention(
        post_id=post_id,
        market_slug=market_slug,
        event_slug=str(market.get("event_slug") or "") or None,
        entities=entities,
        keywords=sorted(set(keyword_hits + action_hits)),
        confidence=round(confidence, 4),
    )


def _entity_hits(post_text: str, market_text: str, groups: Mapping[str, Sequence[str]]) -> set:
    post = post_text.lower()
    market = market_text.lower()
    hits = set()
    for label, aliases in groups.items():
        if any(alias in post for alias in aliases) and any(alias in market for alias in aliases):
            hits.add(str(label))
    return hits


def _json_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(decoded, list):
            return [str(item) for item in decoded]
    return []
