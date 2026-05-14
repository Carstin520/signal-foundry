from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class EndpointPayload:
    endpoint: str
    params: Dict[str, object]
    payload: object


@dataclass(frozen=True)
class WalletMetrics:
    label: str
    address: Optional[str]
    status: str
    analyzed_at: str
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    win_rate: Optional[float]
    closed_markets: int
    open_markets: int
    total_volume: float
    top_category: Optional[str]
    top_category_pnl_share: Optional[float]
    max_market_pnl: float
    max_market_pnl_share: Optional[float]
    late_entry_ratio: Optional[float]
    confidence: str
    risk_tags: List[str]

