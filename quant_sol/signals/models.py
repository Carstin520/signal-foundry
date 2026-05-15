from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class MarketRecord:
    market_slug: str
    event_slug: Optional[str]
    question: str
    category: str
    tags: List[str]
    end_time: Optional[str]
    resolution_source: Optional[str]
    clob_token_ids: List[str]
    liquidity: Optional[float] = None
    raw: Dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class SocialPost:
    platform: str
    handle: str
    post_id: str
    created_at: str
    text: str
    url: str
    raw: Dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class EventMention:
    post_id: str
    market_slug: str
    event_slug: Optional[str]
    entities: List[str]
    keywords: List[str]
    confidence: float


@dataclass(frozen=True)
class SignalScore:
    signal_id: str
    event_family: str
    market_slug: str
    direction_hint: str
    score: int
    confidence: str
    evidence: Dict[str, object]
    risk_tags: List[str]
    source_posts: List[Dict[str, object]]
    wallet_flows: List[Dict[str, object]]
    price_window: Dict[str, object]
