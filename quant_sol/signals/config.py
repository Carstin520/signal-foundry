from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import yaml

from quant_sol.wallets.config import DATA_ROOT, DB_PATH, GAMMA_API_BASE_URL


CONFIG_ROOT = Path("config")
SOCIAL_WATCHLIST_PATH = CONFIG_ROOT / "social_watchlist.yaml"
MARKET_RULES_PATH = CONFIG_ROOT / "market_keyword_rules.yaml"
WALLET_WATCHLIST_PATH = CONFIG_ROOT / "wallet_watchlist.yaml"
FOMO_MODEL_PATH = CONFIG_ROOT / "fomo_model.yaml"
WEB3_ACCOUNT_WATCHLIST_PATH = CONFIG_ROOT / "web3_account_watchlist.yaml"
WEB3_NARRATIVE_KEYWORDS_PATH = CONFIG_ROOT / "web3_narrative_keywords.yaml"
API_LIMITS_PATH = CONFIG_ROOT / "api_limits.yaml"
SIGNAL_RAW_ROOT = DATA_ROOT / "raw" / "signals"
SIGNAL_REPORT_ROOT = DATA_ROOT / "reports"

POLYMARKET_CLOB_BASE_URL = "https://clob.polymarket.com"
POLYMARKET_CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
X_API_BASE_URL = "https://api.x.com/2"
TELEGRAM_API_BASE_URL = "https://api.telegram.org"


@dataclass(frozen=True)
class SocialHandle:
    handle: str
    category: str
    source_score: int
    status: str = "active"


@dataclass(frozen=True)
class MarketRules:
    category: str
    keywords: Tuple[str, ...]
    entities: Mapping[str, Tuple[str, ...]]
    actions: Mapping[str, Tuple[str, ...]]


@dataclass(frozen=True)
class FomoModelConfig:
    ideal_price_min: float
    ideal_price_max: float
    acceptable_price_min: float
    acceptable_price_max: float
    lottery_tail_max: float
    crowded_min: float
    hard_deadline_hours: int
    soft_deadline_days: int
    already_moved_6h: float
    already_moved_24h: float
    positive_outcome: float
    strong_favorable: float
    strong_adverse_max: float
    minimum_liquidity: float
    max_spread: float
    alert_threshold: int
    short_window: str
    medium_window: str
    base_window: str
    bullish_keywords: Tuple[str, ...]
    bearish_keywords: Tuple[str, ...]


@dataclass(frozen=True)
class Web3AccountConfig:
    handle: str
    language: str
    region: str
    role: str
    priority: str
    notes: str = ""


@dataclass(frozen=True)
class Web3NarrativeKeywords:
    groups: Mapping[str, Tuple[str, ...]]
    role_weights: Mapping[str, int]


@dataclass(frozen=True)
class XApiLimits:
    daily_call_cap: int
    sync_social_max_handles: int
    sync_social_max_posts_per_handle: int
    sync_accounts_max_accounts: int
    sync_accounts_max_posts_per_account: int
    sync_follow_graph_max_accounts: int
    sync_follow_graph_max_following_per_account: int


def load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def load_social_handles(path: Path = SOCIAL_WATCHLIST_PATH) -> List[SocialHandle]:
    payload = load_yaml(path)
    categories = payload.get("categories", {})
    if not isinstance(categories, dict):
        return []

    handles: Dict[str, SocialHandle] = {}
    for category, config in categories.items():
        if not isinstance(config, dict):
            continue
        source_score = int(config["source_score"]) if config.get("source_score") is not None else 15
        for handle in config.get("handles") or []:
            normalized = normalize_handle(str(handle))
            if not normalized:
                continue
            # Keep the highest-trust category if a handle is listed twice.
            existing = handles.get(normalized.lower())
            candidate = SocialHandle(handle=normalized, category=str(category), source_score=source_score)
            if existing is None or candidate.source_score > existing.source_score:
                handles[normalized.lower()] = candidate
    return sorted(handles.values(), key=lambda item: item.handle.lower())


def load_market_rules(path: Path = MARKET_RULES_PATH) -> MarketRules:
    payload = load_yaml(path)
    return MarketRules(
        category=str(payload.get("category") or "politics"),
        keywords=tuple(_strings(payload.get("keywords"))),
        entities={str(k): tuple(_strings(v)) for k, v in (payload.get("entities") or {}).items()},
        actions={str(k): tuple(_strings(v)) for k, v in (payload.get("actions") or {}).items()},
    )


def load_fomo_config(path: Path = FOMO_MODEL_PATH) -> FomoModelConfig:
    payload = load_yaml(path)
    price = payload.get("price_bands") or {}
    deadlines = payload.get("deadlines") or {}
    movement = payload.get("movement") or {}
    liquidity = payload.get("liquidity") or {}
    alert = payload.get("alert") or {}
    windows = payload.get("windows") or {}
    direction = payload.get("direction_keywords") or {}
    return FomoModelConfig(
        ideal_price_min=float(price.get("ideal_min", 0.12)),
        ideal_price_max=float(price.get("ideal_max", 0.45)),
        acceptable_price_min=float(price.get("acceptable_min", 0.08)),
        acceptable_price_max=float(price.get("acceptable_max", 0.60)),
        lottery_tail_max=float(price.get("lottery_tail_max", 0.03)),
        crowded_min=float(price.get("crowded_min", 0.70)),
        hard_deadline_hours=int(deadlines.get("hard_reject_hours", 72)),
        soft_deadline_days=int(deadlines.get("soft_penalty_days", 14)),
        already_moved_6h=float(movement.get("already_moved_6h_pp", 0.08)),
        already_moved_24h=float(movement.get("already_moved_24h_pp", 0.15)),
        positive_outcome=float(movement.get("positive_outcome_pp", 0.03)),
        strong_favorable=float(movement.get("strong_favorable_pp", 0.08)),
        strong_adverse_max=float(movement.get("strong_adverse_max_pp", 0.05)),
        minimum_liquidity=float(liquidity.get("minimum", 10_000)),
        max_spread=float(liquidity.get("max_spread", 0.05)),
        alert_threshold=int(alert.get("threshold", 75)),
        short_window=str(windows.get("short", "1h")),
        medium_window=str(windows.get("medium", "6h")),
        base_window=str(windows.get("base", "24h")),
        bullish_keywords=tuple(_strings(direction.get("bullish"))),
        bearish_keywords=tuple(_strings(direction.get("bearish"))),
    )


def load_wallet_watchlist(name: str = "political", path: Path = WALLET_WATCHLIST_PATH) -> Dict[str, str]:
    payload = load_yaml(path)
    group = payload.get(name, {})
    if not isinstance(group, dict):
        return {}
    return {str(label): str(address) for label, address in group.items() if _is_address(str(address))}


def load_web3_accounts(path: Path = WEB3_ACCOUNT_WATCHLIST_PATH) -> List[Web3AccountConfig]:
    payload = load_yaml(path)
    rows = payload.get("accounts") or []
    accounts: List[Web3AccountConfig] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        handle = normalize_handle(str(row.get("handle") or ""))
        if not handle:
            continue
        accounts.append(
            Web3AccountConfig(
                handle=handle,
                language=str(row.get("language") or "mixed"),
                region=str(row.get("region") or "global"),
                role=str(row.get("role") or "fast_curators"),
                priority=str(row.get("priority") or "watch"),
                notes=str(row.get("notes") or ""),
            )
        )
    return sorted(accounts, key=lambda item: item.handle.lower())


def load_web3_keywords(path: Path = WEB3_NARRATIVE_KEYWORDS_PATH) -> Web3NarrativeKeywords:
    payload = load_yaml(path)
    groups: Dict[str, Tuple[str, ...]] = {}
    for group, config in (payload.get("groups") or {}).items():
        terms: List[str] = []
        if isinstance(config, dict):
            for values in config.values():
                terms.extend(_strings(values))
        elif isinstance(config, list):
            terms.extend(_strings(config))
        groups[str(group)] = tuple(dict.fromkeys(terms))
    role_weights = {
        str(role): int((config or {}).get("score_weight", 10))
        for role, config in (payload.get("roles") or {}).items()
        if isinstance(config, dict)
    }
    return Web3NarrativeKeywords(groups=groups, role_weights=role_weights)


def load_x_api_limits(path: Path = API_LIMITS_PATH) -> XApiLimits:
    payload = load_yaml(path)
    x_config = payload.get("x") if isinstance(payload.get("x"), dict) else {}
    sync_social = x_config.get("sync_social") if isinstance(x_config.get("sync_social"), dict) else {}
    sync_accounts = x_config.get("sync_accounts") if isinstance(x_config.get("sync_accounts"), dict) else {}
    sync_follow_graph = x_config.get("sync_follow_graph") if isinstance(x_config.get("sync_follow_graph"), dict) else {}
    return XApiLimits(
        daily_call_cap=int(x_config.get("daily_call_cap", 80)),
        sync_social_max_handles=int(sync_social.get("max_handles", 10)),
        sync_social_max_posts_per_handle=int(sync_social.get("max_posts_per_handle", 20)),
        sync_accounts_max_accounts=int(sync_accounts.get("max_accounts", 5)),
        sync_accounts_max_posts_per_account=int(sync_accounts.get("max_posts_per_account", 20)),
        sync_follow_graph_max_accounts=int(sync_follow_graph.get("max_accounts", 3)),
        sync_follow_graph_max_following_per_account=int(sync_follow_graph.get("max_following_per_account", 50)),
    )


def normalize_handle(handle: str) -> str:
    return handle.strip().lstrip("@")


def keyword_query(handles: Sequence[SocialHandle], rules: MarketRules) -> str:
    parts = [f"from:{handle.handle}" for handle in handles]
    parts.extend(rules.keywords)
    return " OR ".join(dict.fromkeys(part for part in parts if part))


def parse_duration(value: str) -> int:
    text = value.strip().lower()
    if not text:
        raise ValueError("duration cannot be empty")
    unit = text[-1]
    number = float(text[:-1] if unit in {"s", "m", "h", "d"} else text)
    if unit == "d":
        return int(number * 86400)
    if unit == "h":
        return int(number * 3600)
    if unit == "m":
        return int(number * 60)
    if unit == "s":
        return int(number)
    return int(number)


def _strings(value: object) -> Iterable[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip().lower() for item in value if str(item).strip()]


def _is_address(value: str) -> bool:
    if len(value) != 42 or not value.startswith("0x"):
        return False
    try:
        int(value[2:], 16)
    except ValueError:
        return False
    return True
