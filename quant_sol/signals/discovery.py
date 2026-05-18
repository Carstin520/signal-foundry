from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional, Sequence

import duckdb
import requests
import yaml

from .clients import GammaMarketClient, KalshiMarketClient, XApiClient, market_record_from_gamma
from .models import MarketRecord
from .storage import save_raw_payload, upsert_markets, upsert_signal_discovery_sources, upsert_x_posts
from .utils import first_float, stable_hash, to_datetime, utc_now_iso, words


DISCOVERY_STOPWORDS = {
    "will",
    "what",
    "when",
    "where",
    "which",
    "this",
    "that",
    "with",
    "from",
    "have",
    "before",
    "after",
    "market",
    "polymarket",
    "yes",
    "no",
    "the",
    "and",
    "for",
    "are",
    "any",
    "gta",
    "hit",
    "who",
    "how",
    "2026",
    "2027",
    "2028",
}

NARRATIVE_MARKERS = {
    "iran",
    "israel",
    "china",
    "taiwan",
    "trump",
    "election",
    "tariff",
    "sanction",
    "war",
    "ceasefire",
    "deal",
    "peace",
    "nuclear",
    "fed",
    "sec",
    "crypto",
    "bitcoin",
    "ethereum",
    "solana",
    "binance",
    "coinbase",
    "etf",
}

SPORTS_MARKERS = {
    "fifa",
    "cup",
    "world cup",
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "ufc",
    "soccer",
    "football",
    "basketball",
    "baseball",
    "premier",
    "champions",
}

SOURCE_DISCOVERY_V2_PATH = Path("config/source_discovery_v2.yaml")

EDGE_CLASSIFICATION = "narrative_fomo_edge"
PREFLIGHT_STATUS = "public_seed_preflight"
REDDIT_CONTEXT_STATUS = "low_confidence_context"
DISCORD_WATCH_STATUS = "manual_public_watch"
KALSHI_CONTEXT_STATUS = "cross_venue_context"


def discover_signal_source_candidates(
    con: duckdb.DuckDBPyConnection,
    x_client: Optional[XApiClient] = None,
    *,
    max_markets: int = 8,
    max_gamma_pages: int = 2,
    min_liquidity: float = 100_000,
    focus: str = "narrative",
    lookback_seconds: int = 24 * 3600,
    max_posts_per_market: int = 20,
    include_reddit: bool = True,
    reddit_limit: int = 5,
    include_public_seeds: bool = True,
    include_kalshi: bool = True,
    kalshi_limit: int = 10,
    kalshi_max_pages: int = 2,
    source_seed_path: Optional[Path] = None,
    dry_run: bool = False,
) -> dict:
    markets = discover_interesting_markets(
        max_markets=max_markets,
        max_gamma_pages=max_gamma_pages,
        min_liquidity=min_liquidity,
        focus=focus,
    )
    seed_config = load_source_discovery_seed_config(source_seed_path or SOURCE_DISCOVERY_V2_PATH)
    public_seed_candidates = (
        public_seed_candidates_for_markets(markets, seed_config=seed_config) if include_public_seeds else []
    )
    platform_watch = platform_watch_candidates_for_markets(markets, seed_config=seed_config) if include_public_seeds else []
    kalshi_hot_markets = discover_kalshi_hot_markets(max_markets=kalshi_limit, max_pages=kalshi_max_pages) if include_kalshi else []
    kalshi_cross_venue = kalshi_cross_venue_candidates_for_markets(markets, kalshi_hot_markets)
    if dry_run:
        return {
            "markets": markets,
            "x_posts": [],
            "source_candidates": [],
            "reddit_posts": [],
            "public_seed_candidates": public_seed_candidates,
            "platform_watch": platform_watch,
            "kalshi_hot_markets": kalshi_hot_markets,
            "kalshi_cross_venue": kalshi_cross_venue,
            "source_references": seed_config.get("references") or [],
            "planned_x_calls": len(markets),
        }
    upsert_markets(con, [item["record"] for item in markets])
    x_posts = []
    reddit_posts = []
    if x_client is not None:
        for market in markets:
            query = x_query_for_market(market["record"])
            if not query:
                continue
            posts = x_client.recent_search(query, seconds=lookback_seconds, max_results=max_posts_per_market)
            x_posts.extend([{**post, "matched_market_slug": market["record"].market_slug, "search_query": query} for post in posts])
            save_raw_payload("x_market_discovery", market["record"].market_slug, posts)
    if include_reddit:
        reddit = RedditPublicClient()
        for market in markets[: max(0, min(len(markets), 5))]:
            query = reddit_query_for_market(market["record"])
            if not query:
                continue
            posts = reddit.search(query, limit=reddit_limit)
            reddit_posts.extend([{**post, "matched_market_slug": market["record"].market_slug, "search_query": query} for post in posts])
            if posts:
                save_raw_payload("reddit_market_discovery", market["record"].market_slug, posts)
    source_candidates = rank_discovered_sources(x_posts, reddit_posts, markets)
    upsert_x_posts(con, x_posts)
    upsert_signal_discovery_sources(con, source_candidates)
    return {
        "markets": markets,
        "x_posts": x_posts,
        "source_candidates": source_candidates,
        "reddit_posts": reddit_posts,
        "public_seed_candidates": public_seed_candidates,
        "platform_watch": platform_watch,
        "kalshi_hot_markets": kalshi_hot_markets,
        "kalshi_cross_venue": kalshi_cross_venue,
        "source_references": seed_config.get("references") or [],
        "planned_x_calls": len(markets),
    }


def discover_interesting_markets(
    *,
    max_markets: int = 8,
    max_gamma_pages: int = 2,
    min_liquidity: float = 100_000,
    focus: str = "narrative",
) -> list[dict]:
    rows = GammaMarketClient().list_markets(max_pages=max_gamma_pages)
    candidates = []
    now = datetime.now(timezone.utc)
    for item in rows:
        record = market_record_from_gamma(item)
        if not _is_open_market(item, record, now):
            continue
        if not _focus_allows_market(record, focus):
            continue
        if not record.clob_token_ids:
            continue
        liquidity = float(record.liquidity or 0)
        volume = _market_volume(item)
        if liquidity < min_liquidity and volume < min_liquidity:
            continue
        score = market_interest_score(record, item, now)
        if score <= 0:
            continue
        candidates.append(
            {
                "record": record,
                "score": score,
                "liquidity": liquidity,
                "volume": volume,
                "deadline_days": _deadline_days(record, now),
                "query_terms": market_query_terms(record),
            }
        )
    return sorted(candidates, key=lambda row: row["score"], reverse=True)[:max_markets]


def market_interest_score(record: MarketRecord, raw: Mapping[str, object], now: Optional[datetime] = None) -> float:
    now = now or datetime.now(timezone.utc)
    liquidity = float(record.liquidity or 0)
    volume = _market_volume(raw)
    deadline_days = _deadline_days(record, now)
    terms = set(market_query_terms(record))
    narrative_bonus = min(20, len(terms & NARRATIVE_MARKERS) * 4)
    deadline_score = 0.0
    if deadline_days is None:
        deadline_score = 8.0
    elif deadline_days < 1:
        deadline_score = -25.0
    elif deadline_days < 3:
        deadline_score = -8.0
    elif deadline_days <= 180:
        deadline_score = 18.0
    else:
        deadline_score = 10.0
    liquidity_score = min(35.0, math.log10(max(liquidity, 1)) * 5)
    volume_score = min(30.0, math.log10(max(volume, 1)) * 4)
    return round(max(0.0, liquidity_score + volume_score + deadline_score + narrative_bonus), 3)


def kalshi_market_heat_score(raw: Mapping[str, object]) -> float:
    volume_24h = first_float(raw, "volume_24h_fp") or 0.0
    volume = first_float(raw, "volume_fp") or 0.0
    liquidity = first_float(raw, "liquidity_dollars") or 0.0
    open_interest = first_float(raw, "open_interest_fp") or 0.0
    spread = _kalshi_spread(raw)
    terms = set(kalshi_query_terms(raw, max_terms=8))
    narrative_bonus = min(18.0, len(terms & NARRATIVE_MARKERS) * 3.5)
    sports_penalty = -8.0 if terms & {term for marker in SPORTS_MARKERS for term in marker.split()} else 0.0
    spread_penalty = min(12.0, max(0.0, float(spread or 0) * 24.0))
    score = (
        min(36.0, math.log10(max(volume_24h, 1)) * 7.0)
        + min(24.0, math.log10(max(volume, 1)) * 4.5)
        + min(18.0, math.log10(max(liquidity, 1)) * 3.5)
        + min(14.0, math.log10(max(open_interest, 1)) * 3.0)
        + narrative_bonus
        + sports_penalty
        - spread_penalty
    )
    return round(max(0.0, score), 3)


def _focus_allows_market(record: MarketRecord, focus: str) -> bool:
    normalized = focus.lower()
    haystack = " ".join([record.question, record.market_slug, record.event_slug or "", record.category or "", " ".join(record.tags)]).lower()
    is_sports = any(marker in haystack for marker in SPORTS_MARKERS)
    has_narrative = any(marker in haystack for marker in NARRATIVE_MARKERS)
    if normalized == "all":
        return True
    if normalized == "sports":
        return is_sports
    if normalized == "crypto":
        return any(marker in haystack for marker in {"crypto", "bitcoin", "ethereum", "solana", "binance", "coinbase", "etf", "sec"})
    if normalized in {"politics", "geopolitics"}:
        return any(marker in haystack for marker in {"iran", "israel", "china", "taiwan", "trump", "election", "tariff", "sanction", "war", "ceasefire", "nuclear"})
    return has_narrative and not is_sports


def market_query_terms(record: MarketRecord, max_terms: int = 5) -> list[str]:
    haystack = " ".join([record.question, record.market_slug, record.event_slug or "", " ".join(record.tags)])
    counts = Counter(term for term in words(haystack) if len(term) > 2 and not term.isdigit() and term not in DISCOVERY_STOPWORDS)
    prioritized = sorted(counts, key=lambda term: ((term not in NARRATIVE_MARKERS), -counts[term], term))
    return prioritized[:max_terms]


def kalshi_query_terms(raw: Mapping[str, object], max_terms: int = 6) -> list[str]:
    haystack = " ".join(
        [
            str(raw.get("title") or ""),
            str(raw.get("subtitle") or ""),
            str(raw.get("yes_sub_title") or ""),
            str(raw.get("no_sub_title") or ""),
            str(raw.get("event_ticker") or ""),
            str(raw.get("ticker") or ""),
        ]
    )
    counts = Counter(term for term in words(haystack) if len(term) > 2 and not term.isdigit() and term not in DISCOVERY_STOPWORDS)
    prioritized = sorted(counts, key=lambda term: ((term not in NARRATIVE_MARKERS), -counts[term], term))
    return prioritized[:max_terms]


def _kalshi_category(raw: Mapping[str, object]) -> str:
    return _target_category_from_text(
        " ".join(
            [
                str(raw.get("title") or ""),
                str(raw.get("subtitle") or ""),
                str(raw.get("yes_sub_title") or ""),
                str(raw.get("no_sub_title") or ""),
                str(raw.get("event_ticker") or ""),
                str(raw.get("ticker") or ""),
                str(raw.get("category") or ""),
            ]
        )
    )


def x_query_for_market(record: MarketRecord) -> str:
    terms = market_query_terms(record, max_terms=4)
    if not terms:
        return ""
    query = " ".join(terms)
    return f"({query}) -is:retweet"


def reddit_query_for_market(record: MarketRecord) -> str:
    return " ".join(market_query_terms(record, max_terms=4))


def load_source_discovery_seed_config(path: Path = SOURCE_DISCOVERY_V2_PATH) -> dict:
    if not path.exists():
        return {"seeds": [], "platform_watch": {}, "references": []}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {"seeds": [], "platform_watch": {}, "references": []}


def public_seed_candidates_for_markets(
    markets: Sequence[Mapping[str, object]],
    *,
    seed_config: Optional[Mapping[str, object]] = None,
    max_per_market: int = 12,
) -> list[dict]:
    config = dict(seed_config or load_source_discovery_seed_config())
    seeds = [seed for seed in config.get("seeds") or [] if isinstance(seed, dict)]
    rows: list[dict] = []
    run_id = discovery_run_id()
    for item in markets:
        record = item["record"]
        market_terms = set(str(term).lower() for term in (item.get("query_terms") or market_query_terms(record)))
        haystack = _market_haystack(record)
        matched = []
        for seed in seeds:
            terms = set(str(term).lower() for term in (seed.get("themes") or []))
            overlap = sorted((market_terms | set(words(haystack))) & terms)
            role = str(seed.get("role") or "")
            if not overlap and role != "account_dependency":
                continue
            if role == "account_dependency" and not overlap and not _account_dependency_market(record):
                continue
            matched.append((_seed_preflight_score(item, seed, overlap), seed, overlap))
        matched.sort(key=lambda row: row[0], reverse=True)
        for score, seed, overlap in matched[:max_per_market]:
            rows.append(_public_seed_row(run_id, item, seed, overlap, score))
    return rows


def platform_watch_candidates_for_markets(
    markets: Sequence[Mapping[str, object]],
    *,
    seed_config: Optional[Mapping[str, object]] = None,
    max_per_market: int = 8,
) -> list[dict]:
    config = dict(seed_config or load_source_discovery_seed_config())
    watch = config.get("platform_watch") if isinstance(config.get("platform_watch"), dict) else {}
    rows: list[dict] = []
    run_id = discovery_run_id()
    for item in markets:
        record = item["record"]
        terms = set(str(term).lower() for term in (item.get("query_terms") or market_query_terms(record)))
        market_words = terms | set(words(_market_haystack(record)))
        for platform, entries in watch.items():
            matched_entries = []
            for entry in entries or []:
                if not isinstance(entry, dict):
                    continue
                entry_terms = set(str(term).lower() for term in (entry.get("themes") or []))
                overlap = sorted(market_words & entry_terms)
                if overlap or platform == "discord":
                    matched_entries.append((len(overlap), entry, overlap))
            matched_entries.sort(key=lambda row: row[0], reverse=True)
            for _, entry, overlap in matched_entries[:max_per_market]:
                rows.append(_platform_watch_row(run_id, item, str(platform), entry, overlap))
    return rows


def discover_kalshi_hot_markets(
    *,
    max_markets: int = 10,
    max_pages: int = 2,
    client: Optional[KalshiMarketClient] = None,
) -> list[dict]:
    """Fetch public Kalshi markets and rank them as cross-venue discussion context."""
    if max_markets <= 0:
        return []
    client = client or KalshiMarketClient()
    try:
        rows = client.list_markets(limit=1000, max_pages=max_pages, status="open", mve_filter="exclude")
    except requests.RequestException:
        return []
    candidates = []
    now = datetime.now(timezone.utc)
    for raw in rows:
        score = kalshi_market_heat_score(raw)
        if score <= 0:
            continue
        close_time = to_datetime(str(raw.get("close_time") or raw.get("expiration_time") or ""))
        deadline_days = max(0.0, (close_time - now).total_seconds() / 86400) if close_time else None
        candidates.append(
            {
                "venue": "kalshi",
                "ticker": raw.get("ticker") or "",
                "event_ticker": raw.get("event_ticker") or "",
                "title": raw.get("title") or raw.get("yes_sub_title") or "",
                "category": _kalshi_category(raw),
                "heat_score": score,
                "volume_24h": first_float(raw, "volume_24h_fp") or 0.0,
                "volume": first_float(raw, "volume_fp") or 0.0,
                "liquidity": first_float(raw, "liquidity_dollars") or 0.0,
                "open_interest": first_float(raw, "open_interest_fp") or 0.0,
                "last_price": first_float(raw, "last_price_dollars"),
                "yes_bid": first_float(raw, "yes_bid_dollars"),
                "yes_ask": first_float(raw, "yes_ask_dollars"),
                "spread": _kalshi_spread(raw),
                "deadline_days": deadline_days,
                "close_time": raw.get("close_time") or raw.get("expiration_time"),
                "query_terms": kalshi_query_terms(raw),
                "edge_classification": "cross_venue_context",
                "data_provenance": {
                    "source_activity": "observed_kalshi_public_market_api",
                    "market_selection": "model_derived_public_heat_filter",
                    "price_impact": "not_evaluated_against_polymarket",
                    "platform_context": "observed_cross_venue_market_context",
                },
                "tradability": {
                    "status": "context_only_until_rules_and_spread_match",
                    "required_check": "compare Kalshi rules, close time, bid/ask, and volume against Polymarket equivalent",
                },
                "risk_tags": ["cross_venue_context", "venue_rule_mismatch", "no_order_execution"],
                "raw": raw,
            }
        )
    return sorted(candidates, key=lambda row: row["heat_score"], reverse=True)[:max_markets]


def kalshi_cross_venue_candidates_for_markets(
    markets: Sequence[Mapping[str, object]],
    kalshi_hot_markets: Sequence[Mapping[str, object]],
    *,
    max_per_market: int = 5,
) -> list[dict]:
    rows: list[dict] = []
    run_id = discovery_run_id()
    for item in markets:
        record = item["record"]
        market_words = set(str(term).lower() for term in (item.get("query_terms") or market_query_terms(record))) | set(
            words(_market_haystack(record))
        )
        matched = []
        for kalshi in kalshi_hot_markets:
            kalshi_terms = set(str(term).lower() for term in (kalshi.get("query_terms") or []))
            overlap = sorted(market_words & kalshi_terms)
            if not overlap:
                continue
            matched.append((len(overlap), float(kalshi.get("heat_score") or 0), kalshi, overlap))
        matched.sort(key=lambda row: (row[0], row[1]), reverse=True)
        for _, _, kalshi, overlap in matched[:max_per_market]:
            rows.append(_kalshi_cross_venue_row(run_id, item, kalshi, overlap))
    return rows


def rank_discovered_sources(x_posts: Sequence[dict], reddit_posts: Sequence[dict], markets: Sequence[dict]) -> list[dict]:
    market_scores = {item["record"].market_slug: item["score"] for item in markets}
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for post in x_posts:
        handle = str(post.get("handle") or "").lstrip("@")
        market_slug = str(post.get("matched_market_slug") or "")
        if handle and market_slug:
            grouped[(handle, market_slug)].append(post)
    reddit_by_market = Counter(str(post.get("matched_market_slug") or "") for post in reddit_posts)
    rows = []
    for (handle, market_slug), posts in grouped.items():
        metrics = [_public_metrics(post) for post in posts]
        engagement = sum(metric.get("like_count", 0) + metric.get("retweet_count", 0) * 2 + metric.get("reply_count", 0) for metric in metrics)
        earliest = min(str(post.get("created_at")) for post in posts if post.get("created_at"))
        unique_posts = {str(post.get("post_id")) for post in posts if post.get("post_id")}
        reddit_context = reddit_by_market.get(market_slug, 0)
        score = min(
            100.0,
            len(unique_posts) * 12
            + math.log1p(max(engagement, 0)) * 7
            + min(20.0, market_scores.get(market_slug, 0) * 0.18)
            + min(8.0, reddit_context * 2),
        )
        status = "candidate_signal_source" if score >= 45 and (len(unique_posts) >= 2 or engagement >= 25) else "watch"
        risk_tags = ["x_observed_posts"]
        if len(unique_posts) <= 1:
            risk_tags.append("single_source_narrative")
        if reddit_context:
            risk_tags.append("reddit_context_available")
        if status == "watch":
            risk_tags.append("needs_repeated_post_to_price_evidence")
        rows.append(
            {
                "run_id": discovery_run_id(),
                "platform": "x",
                "handle": handle,
                "market_slug": market_slug,
                "first_seen_at": earliest,
                "post_count": len(unique_posts),
                "engagement_score": engagement,
                "discovery_score": round(score, 3),
                "recommended_status": status,
                "edge_classification": EDGE_CLASSIFICATION if status == "candidate_signal_source" else "unvalidated_narrative_context",
                "data_provenance": {
                    "source_activity": "observed_x_recent_search",
                    "market_selection": "model_derived_gamma_filter",
                    "price_impact": "not_evaluated_in_discovery",
                    "platform_context": "observed_reddit_public_search" if reddit_context else "not_observed",
                },
                "participant_lens": _participant_lens_for_seed(role="observed_x_candidate"),
                "tradability": {
                    "status": "needs_price_backtest",
                    "cost_first_failure": "unknown_until_spread_liquidity_check",
                    "required_check": "compare post time with pre/post price path, spread, liquidity, and already-moved flags",
                },
                "risk_tags": risk_tags,
                "required_data": [
                    "x post timestamp",
                    "pre/post market ticks",
                    "bid/ask spread",
                    "liquidity/depth",
                    "source repeatability sample",
                ],
                "failure_mode": "engagement or Reddit context can identify attention without proving tradable lead time",
                "evidence": {
                    "post_ids": sorted(unique_posts)[:10],
                    "reddit_context_posts": reddit_context,
                    "market_score": market_scores.get(market_slug),
                    "risk_tags": risk_tags,
                },
            }
        )
    return sorted(rows, key=lambda row: row["discovery_score"], reverse=True)


def _public_seed_row(run_id: str, item: Mapping[str, object], seed: Mapping[str, object], overlap: Sequence[str], score: float) -> dict:
    record = item["record"]
    role = str(seed.get("role") or "source_seed")
    risk_tags = list(dict.fromkeys(str(tag) for tag in (seed.get("risk_tags") or [])))
    if "social_only_preflight" not in risk_tags:
        risk_tags.append("social_only_preflight")
    if role == "account_dependency" and "account_dependency" not in risk_tags:
        risk_tags.append("account_dependency")
    return {
        "run_id": run_id,
        "platform": str(seed.get("platform") or "x"),
        "handle": str(seed.get("handle") or seed.get("name") or "").lstrip("@"),
        "market_slug": record.market_slug,
        "first_seen_at": None,
        "post_count": 0,
        "engagement_score": 0.0,
        "discovery_score": round(score, 3),
        "recommended_status": PREFLIGHT_STATUS,
        "edge_classification": EDGE_CLASSIFICATION,
        "data_provenance": {
            "source_activity": "public_seed_config",
            "market_selection": "model_derived_gamma_filter",
            "price_impact": "not_evaluated_in_preflight",
            "platform_context": "inferred_theme_match",
        },
        "participant_lens": _participant_lens_for_seed(role=role),
        "tradability": _tradability_preflight(item, risk_tags),
        "risk_tags": risk_tags,
        "required_data": [
            "fresh public post timestamp",
            "pre/post market ticks",
            "bid/ask spread",
            "liquidity/depth",
            "independent confirmation where applicable",
        ],
        "failure_mode": _seed_failure_mode(role, risk_tags),
        "evidence": {
            "source": "public_seed_config",
            "role": role,
            "priority": seed.get("priority") or "watch",
            "matched_terms": list(overlap),
            "notes": seed.get("notes") or "",
            "risk_tags": risk_tags,
        },
    }


def _platform_watch_row(
    run_id: str,
    item: Mapping[str, object],
    platform: str,
    entry: Mapping[str, object],
    overlap: Sequence[str],
) -> dict:
    record = item["record"]
    status = DISCORD_WATCH_STATUS if platform == "discord" else REDDIT_CONTEXT_STATUS
    risk_tags = list(dict.fromkeys(str(tag) for tag in (entry.get("risk_tags") or [])))
    if platform == "reddit" and "low_confidence_context" not in risk_tags:
        risk_tags.append("low_confidence_context")
    if platform == "discord" and "no_private_scraping" not in risk_tags:
        risk_tags.append("no_private_scraping")
    return {
        "run_id": run_id,
        "platform": platform,
        "handle": str(entry.get("name") or ""),
        "market_slug": record.market_slug,
        "discovery_score": round(min(40.0, 12.0 + len(overlap) * 4.0 + float(item.get("score") or 0) * 0.05), 3),
        "recommended_status": status,
        "edge_classification": "context_only_until_post_to_price_validated",
        "data_provenance": {
            "source_activity": "public_platform_watch_config",
            "market_selection": "model_derived_gamma_filter",
            "price_impact": "not_evaluated_in_preflight",
            "platform_context": "observed_public_forum_context" if platform == "reddit" else "inferred_authorized_channel_only",
        },
        "participant_lens": _participant_lens_for_seed(role=f"{platform}_context"),
        "tradability": {
            "status": "not_tradable_without_x_or_tick_confirmation",
            "cost_first_failure": "unverified_lead_time",
            "required_check": "link platform post time to market ticks and spread/liquidity before promotion",
        },
        "risk_tags": risk_tags,
        "required_data": [
            "public or authorized post timestamp",
            "source/channel identity",
            "pre/post market ticks",
            "spread/liquidity snapshot",
        ],
        "failure_mode": "discussion velocity can follow price instead of leading it",
        "evidence": {
            "source": "platform_watch_config",
            "matched_terms": list(overlap),
            "access": entry.get("access") or "public",
            "risk_tags": risk_tags,
        },
    }


def _kalshi_cross_venue_row(
    run_id: str,
    item: Mapping[str, object],
    kalshi: Mapping[str, object],
    overlap: Sequence[str],
) -> dict:
    record = item["record"]
    risk_tags = ["cross_venue_context", "venue_rule_mismatch", "no_order_execution", "rules_must_be_compared"]
    return {
        "run_id": run_id,
        "venue": "kalshi",
        "platform": "kalshi",
        "handle": str(kalshi.get("ticker") or ""),
        "market_slug": record.market_slug,
        "kalshi_ticker": kalshi.get("ticker") or "",
        "kalshi_title": kalshi.get("title") or "",
        "discovery_score": round(min(60.0, 18.0 + len(overlap) * 6.0 + float(kalshi.get("heat_score") or 0) * 0.12), 3),
        "recommended_status": KALSHI_CONTEXT_STATUS,
        "edge_classification": "cross_venue_context_until_post_to_price_validated",
        "data_provenance": {
            "source_activity": "observed_kalshi_public_market_api",
            "market_selection": "model_derived_gamma_filter_plus_kalshi_heat_filter",
            "price_impact": "not_evaluated_against_polymarket",
            "platform_context": "observed_cross_venue_market_context",
        },
        "participant_lens": {
            "retail": "context_only_rule_mismatch_risk",
            "institution": "cross_venue_attention_and_capacity_context",
            "market_maker": "watch_for_cross_venue_adverse_selection",
        },
        "tradability": {
            "status": "not_tradable_without_rule_mapping_and_spread_check",
            "cost_first_failure": "venue_rule_or_liquidity_mismatch",
            "required_check": "map resolution rules, close time, bid/ask spread, liquidity, and already-moved flags across venues",
        },
        "risk_tags": risk_tags,
        "required_data": [
            "Kalshi market ticker/title/rules",
            "Kalshi bid/ask, volume, open interest",
            "Polymarket comparable market rules",
            "Polymarket pre/post ticks and spread",
        ],
        "failure_mode": "a hot Kalshi pool can reflect a different contract, user base, fee model, or resolution source than the Polymarket pool",
        "evidence": {
            "source": "kalshi_public_market_api",
            "matched_terms": list(overlap),
            "kalshi_heat_score": kalshi.get("heat_score"),
            "kalshi_volume_24h": kalshi.get("volume_24h"),
            "kalshi_spread": kalshi.get("spread"),
            "risk_tags": risk_tags,
        },
    }


def _seed_preflight_score(item: Mapping[str, object], seed: Mapping[str, object], overlap: Sequence[str]) -> float:
    priority_bonus = {"core": 14.0, "watch": 7.0, "seed": 10.0}.get(str(seed.get("priority") or "watch"), 5.0)
    role_bonus = 8.0 if str(seed.get("role") or "") == "account_dependency" else 0.0
    market_score = float(item.get("score") or 0)
    return min(80.0, 20.0 + priority_bonus + role_bonus + len(overlap) * 5.0 + market_score * 0.12)


def _tradability_preflight(item: Mapping[str, object], risk_tags: Sequence[str]) -> dict:
    liquidity = float(item.get("liquidity") or 0)
    deadline_days = item.get("deadline_days")
    if liquidity and liquidity < 25_000:
        status = "blocked"
        first_failure = "liquidity"
    elif isinstance(deadline_days, (int, float)) and deadline_days < 1:
        status = "blocked"
        first_failure = "deadline"
    else:
        status = "needs_live_validation"
        first_failure = "unknown_until_spread_slippage_check"
    return {
        "status": status,
        "cost_first_failure": first_failure,
        "tradability_score": max(0, min(100, int(math.log10(max(liquidity, 1)) * 12))),
        "required_check": "fresh post age, spread, depth, and pre/post price path must pass before promotion",
        "preflight_only": True,
        "risk_tags": list(risk_tags),
    }


def _participant_lens_for_seed(*, role: str) -> dict:
    if role == "account_dependency":
        return {
            "retail": "watch_only_high_reflexivity",
            "institution": "context_until_liquidity_and_capacity_pass",
            "market_maker": "adverse_selection_watch",
        }
    if role.endswith("_context") or role in {"reddit_context", "discord_context"}:
        return {
            "retail": "context_only",
            "institution": "sentiment_context_only",
            "market_maker": "flow_attention_context",
        }
    return {
        "retail": "candidate_after_simple_entry_exit_check",
        "institution": "candidate_after_capacity_and_repeatability_check",
        "market_maker": "watch_for_inventory_and_adverse_selection",
    }


def _seed_failure_mode(role: str, risk_tags: Sequence[str]) -> str:
    if role == "account_dependency":
        return "a single account can define the narrative, but the market may move before the post is captured"
    if "official_confirmation" in risk_tags:
        return "official posts often confirm after price already moved"
    if "fast_but_noisy" in risk_tags:
        return "fast headline flow can create false FOMO without settlement-relevant evidence"
    return "public seed relevance is inferred until repeated post-to-price samples validate lead time"


def _market_haystack(record: MarketRecord) -> str:
    return " ".join([record.question, record.market_slug, record.event_slug or "", record.category or "", " ".join(record.tags)]).lower()


def _account_dependency_market(record: MarketRecord) -> bool:
    haystack = _market_haystack(record)
    markers = {"say", "tweet", "post", "elon", "musk", "tesla", "nvidia", "cz", "binance", "account"}
    return any(marker in haystack for marker in markers)


def write_signal_discovery_report(result: Mapping[str, object], report_root: Path) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    path = report_root / f"signal_source_discovery_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
    markets = list(result.get("markets") or [])
    sources = list(result.get("source_candidates") or [])
    reddit_posts = list(result.get("reddit_posts") or [])
    public_seeds = list(result.get("public_seed_candidates") or [])
    platform_watch = list(result.get("platform_watch") or [])
    kalshi_hot_markets = list(result.get("kalshi_hot_markets") or [])
    kalshi_cross_venue = list(result.get("kalshi_cross_venue") or [])
    references = list(result.get("source_references") or [])
    lines = [
        "# Signal Source Discovery",
        "",
        f"- Generated at: {utc_now_iso()}",
        f"- Markets scanned: {len(markets)}",
        f"- X posts collected: {len(result.get('x_posts') or [])}",
        f"- Reddit context posts collected: {len(reddit_posts)}",
        f"- Public seed candidates: {len(public_seeds)}",
        f"- Platform watch candidates: {len(platform_watch)}",
        f"- Kalshi hot markets: {len(kalshi_hot_markets)}",
        f"- Kalshi cross-venue matches: {len(kalshi_cross_venue)}",
        f"- Source candidates: {len(sources)}",
        "",
        "## Market Candidates",
        "",
        "| Rank | Market | Score | Liquidity | Volume | Deadline Days | Query Terms |",
        "| ---: | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for idx, item in enumerate(markets, start=1):
        record = item["record"]
        lines.append(
            f"| {idx} | `{record.market_slug}` | {_fmt(item.get('score'))} | {_fmt(item.get('liquidity'))} | "
            f"{_fmt(item.get('volume'))} | {_fmt(item.get('deadline_days'))} | {', '.join(item.get('query_terms') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Public Seed Preflight",
            "",
            "| Rank | Platform | Source | Market | Score | Status | Edge | Risk Tags |",
            "| ---: | --- | --- | --- | ---: | --- | --- | --- |",
        ]
    )
    if not public_seeds:
        lines.append("| n/a | n/a | n/a | n/a | 0 | no_public_seeds | n/a | n/a |")
    for idx, row in enumerate(sorted(public_seeds, key=lambda item: item.get("discovery_score") or 0, reverse=True)[:60], start=1):
        lines.append(
            f"| {idx} | {row.get('platform')} | {_source_label(row)} | `{row.get('market_slug')}` | "
            f"{_fmt(row.get('discovery_score'))} | {row.get('recommended_status')} | "
            f"{row.get('edge_classification')} | {', '.join(row.get('risk_tags') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Source Candidates",
            "",
            "| Rank | Platform | Handle | Market | Score | Posts | Engagement | Status | Edge | Tradability |",
            "| ---: | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    if not sources:
        lines.append("| n/a | n/a | n/a | n/a | 0 | 0 | 0 | no_candidates | n/a | n/a |")
    for idx, row in enumerate(sources[:50], start=1):
        tradability = row.get("tradability") if isinstance(row.get("tradability"), dict) else {}
        lines.append(
            f"| {idx} | {row.get('platform')} | @{row.get('handle')} | `{row.get('market_slug')}` | "
            f"{_fmt(row.get('discovery_score'))} | {row.get('post_count')} | {_fmt(row.get('engagement_score'))} | "
            f"{row.get('recommended_status')} | {row.get('edge_classification')} | {tradability.get('status') or 'n/a'} |"
        )
    lines.extend(
        [
            "",
            "## Reddit And Discord Watch",
            "",
            "| Rank | Platform | Source | Market | Score | Status | Access/Risk |",
            "| ---: | --- | --- | --- | ---: | --- | --- |",
        ]
    )
    if not platform_watch:
        lines.append("| n/a | n/a | n/a | n/a | 0 | no_platform_watch | n/a |")
    for idx, row in enumerate(sorted(platform_watch, key=lambda item: item.get("discovery_score") or 0, reverse=True)[:60], start=1):
        lines.append(
            f"| {idx} | {row.get('platform')} | {_source_label(row)} | `{row.get('market_slug')}` | "
            f"{_fmt(row.get('discovery_score'))} | {row.get('recommended_status')} | "
            f"{', '.join(row.get('risk_tags') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Kalshi Hot Pool Context",
            "",
            "| Rank | Ticker | Category | Heat | Volume 24h | Liquidity | Spread | Terms |",
            "| ---: | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    if not kalshi_hot_markets:
        lines.append("| n/a | n/a | n/a | 0 | 0 | 0 | n/a | n/a |")
    for idx, row in enumerate(kalshi_hot_markets[:40], start=1):
        lines.append(
            f"| {idx} | `{row.get('ticker')}` | {row.get('category') or 'other'} | {_fmt(row.get('heat_score'))} | "
            f"{_fmt(row.get('volume_24h'))} | {_fmt(row.get('liquidity'))} | {_fmt(row.get('spread'))} | "
            f"{', '.join(row.get('query_terms') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Kalshi Cross-Venue Matches",
            "",
            "| Rank | Kalshi Ticker | Polymarket | Score | Status | Risk Tags |",
            "| ---: | --- | --- | ---: | --- | --- |",
        ]
    )
    if not kalshi_cross_venue:
        lines.append("| n/a | n/a | n/a | 0 | no_cross_venue_match | n/a |")
    for idx, row in enumerate(sorted(kalshi_cross_venue, key=lambda item: item.get("discovery_score") or 0, reverse=True)[:60], start=1):
        lines.append(
            f"| {idx} | `{row.get('kalshi_ticker') or row.get('handle')}` | `{row.get('market_slug')}` | "
            f"{_fmt(row.get('discovery_score'))} | {row.get('recommended_status')} | "
            f"{', '.join(row.get('risk_tags') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Model Review Discipline",
            "",
            "- Edge classes separate narrative/FOMO, liquidity/latency, statistical, model-relative, and true no-arbitrage claims.",
            "- Public seeds are preflight hints. They do not become signal sources until repeated post-to-price evidence exists.",
            "- Reddit is low-confidence context; Discord is manual/public-or-authorized watch only unless explicit API access is provided.",
            "- Kalshi hot pools are cross-venue context. They require rule, deadline, spread, and liquidity mapping before any Polymarket inference.",
            "- Every candidate still needs provenance, required data, failure mode, spread/liquidity, and already-moved checks.",
            "- Participant lens: retail needs simple capped-risk timing, institutions need repeatability/capacity, and market makers watch adverse selection.",
        ]
    )
    if references:
        lines.extend(["", "## Public References", "", "| Label | URL |", "| --- | --- |"])
        for ref in references:
            if isinstance(ref, Mapping):
                lines.append(f"| {ref.get('label') or 'reference'} | {ref.get('url') or ''} |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This discovery report is for candidate sourcing, not trading.",
            "- X searches are intentionally bounded by market count and posts per market.",
            "- Reddit is used as broad context/discussion discovery, not as a high-confidence signal source.",
            "- Discord channels are not scraped unless they are public or explicitly authorized.",
            "- Kalshi hot pools are included as read-only cross-venue context, not as execution targets.",
            "- Candidates should be promoted to watchlists only after repeated post-to-price evidence.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_latest_polymarket_targets(result: Mapping[str, object], path: Path, *, max_targets: int = 10) -> Path:
    """Overwrite a concise working list of current Polymarket targets."""
    path.parent.mkdir(parents=True, exist_ok=True)
    markets = _diversified_markets(list(result.get("markets") or []), max_targets=max_targets)
    sources = list(result.get("source_candidates") or [])
    sources_by_market: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in sources:
        sources_by_market[str(row.get("market_slug") or "")].append(row)

    lines = [
        "# Latest Polymarket Targets",
        "",
        "This file is overwritten by the combined Polymarket discovery heartbeat every 2 hours.",
        "",
        f"- Last updated: {utc_now_iso()}",
        f"- Markets scanned: {len(result.get('markets') or [])}",
        f"- X posts collected: {len(result.get('x_posts') or [])}",
        f"- Reddit context posts collected: {len(result.get('reddit_posts') or [])}",
        f"- Source candidates: {len(sources)}",
        "",
        "## Selection Rules",
        "",
        "- Prefer high-liquidity or high-volume pools with enough depth for realistic entry and exit.",
        "- Keep category diversity across politics, geopolitics, macro, crypto, regulation, sports, and other live narrative areas.",
        "- Prefer markets with narrative uncertainty and enough time left for FOMO-style repricing.",
        "- Downgrade thin pools, near-deadline binaries, stale official-confirmation markets, and crowded markets unless studying overreaction.",
        "",
        "## Current Targets",
        "",
        "| Rank | Category | Market | Score | Liquidity | Volume | Deadline Days | Top Source Candidates | Query Terms |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    if not markets:
        lines.append("| n/a | n/a | n/a | 0 | 0 | 0 | n/a | n/a | n/a |")
    for idx, item in enumerate(markets, start=1):
        record = item["record"]
        market_sources = sources_by_market.get(record.market_slug, [])[:3]
        handles = ", ".join(f"@{row.get('handle')}" for row in market_sources) or "n/a"
        category = _target_category(record)
        lines.append(
            f"| {idx} | {category} | `{record.market_slug}` | {_fmt(item.get('score'))} | "
            f"{_fmt(item.get('liquidity'))} | {_fmt(item.get('volume'))} | {_fmt(item.get('deadline_days'))} | "
            f"{handles} | {', '.join(item.get('query_terms') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Run Notes",
            "",
            "- This list is for research monitoring only, not trading.",
            "- Candidate source handles are discovery hints; promotion requires repeated post-to-price evidence.",
            "- Liquidity and volume are used as depth proxies, but every burst still needs spread and orderbook checks.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _diversified_markets(markets: Sequence[Mapping[str, object]], *, max_targets: int) -> list[Mapping[str, object]]:
    selected = []
    selected_categories = set()
    for item in markets:
        record = item["record"]
        category = _target_category(record).lower()
        if category in selected_categories:
            continue
        selected.append(item)
        selected_categories.add(category)
        if len(selected) >= max_targets:
            return selected
    for item in markets:
        record = item["record"]
        if record.market_slug in {row["record"].market_slug for row in selected}:
            continue
        selected.append(item)
        if len(selected) >= max_targets:
            break
    return selected


def _target_category(record: MarketRecord) -> str:
    haystack = " ".join([record.question, record.market_slug, record.event_slug or "", record.category or "", " ".join(record.tags)]).lower()
    terms = set(words(haystack))
    if terms & {"bitcoin", "crypto", "ethereum", "solana", "binance", "coinbase", "etf"}:
        return "crypto"
    if terms & {"china", "taiwan", "iran", "israel", "war", "ceasefire", "nuclear", "sanction"}:
        return "geopolitics"
    if terms & {"trump", "president", "presidential", "democratic", "republican", "nomination", "election"}:
        return "us_politics"
    if terms & {"fed", "rate", "inflation", "tariff", "recession"}:
        return "macro_policy"
    if terms & {"fifa", "nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "basketball", "baseball"} or "world cup" in haystack:
        return "sports"
    return (record.category or "other").lower().replace(" ", "_")


def discover_kalshi_targets(
    *,
    max_markets: int = 12,
    max_pages: int = 2,
    min_volume: float = 100_000,
    focus: str = "narrative",
) -> dict:
    rows = KalshiMarketClient().list_markets(max_pages=max_pages, status="open")
    now = datetime.now(timezone.utc)
    markets = []
    for row in rows:
        if not _focus_allows_text(_kalshi_text(row), focus):
            continue
        volume = _kalshi_float(row, "volume_24h", "volume_24h_fp", "volume", "volume_fp")
        open_interest = _kalshi_float(row, "open_interest", "open_interest_fp")
        if volume < min_volume and open_interest < min_volume:
            continue
        score = kalshi_interest_score(row, now)
        if score <= 0:
            continue
        markets.append(
            {
                "record": row,
                "score": score,
                "volume": volume,
                "open_interest": open_interest,
                "spread": _kalshi_spread(row),
                "deadline_days": _kalshi_deadline_days(row, now),
                "query_terms": _kalshi_query_terms(row),
                "category": _target_category_from_text(_kalshi_text(row)),
            }
        )
    return {"markets": sorted(markets, key=lambda item: item["score"], reverse=True)[:max_markets]}


def kalshi_interest_score(row: Mapping[str, object], now: Optional[datetime] = None) -> float:
    now = now or datetime.now(timezone.utc)
    volume = _kalshi_float(row, "volume_24h", "volume_24h_fp", "volume", "volume_fp")
    open_interest = _kalshi_float(row, "open_interest", "open_interest_fp")
    deadline_days = _kalshi_deadline_days(row, now)
    spread = _kalshi_spread(row)
    terms = set(_kalshi_query_terms(row))
    narrative_bonus = min(20.0, len(terms & NARRATIVE_MARKERS) * 4)
    volume_score = min(35.0, math.log10(max(volume, 1)) * 5)
    interest_score = min(25.0, math.log10(max(open_interest, 1)) * 4)
    spread_score = 8.0 if spread is None else max(0.0, 12.0 - spread * 100)
    if deadline_days is None:
        deadline_score = 8.0
    elif deadline_days < 1:
        deadline_score = -25.0
    elif deadline_days < 3:
        deadline_score = -8.0
    elif deadline_days <= 180:
        deadline_score = 15.0
    else:
        deadline_score = 8.0
    return round(max(0.0, volume_score + interest_score + spread_score + deadline_score + narrative_bonus), 3)


def write_latest_kalshi_targets(result: Mapping[str, object], path: Path, *, max_targets: int = 10) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    markets = _diversified_kalshi_markets(list(result.get("markets") or []), max_targets=max_targets)
    lines = [
        "# Latest Kalshi Targets",
        "",
        "This file is overwritten by the combined prediction-market discovery heartbeat every 2 hours.",
        "",
        f"- Last updated: {utc_now_iso()}",
        f"- Markets scanned: {len(result.get('markets') or [])}",
        "- Venue: Kalshi public market data",
        "",
        "## Selection Rules",
        "",
        "- Prefer open Kalshi markets with high volume or open interest.",
        "- Keep category diversity across politics, geopolitics, macro, crypto, regulation, and other information-sensitive areas.",
        "- Treat Kalshi candidates as cross-venue research targets; do not infer Polymarket equivalence without separate matching.",
        "- Do not use trading APIs, private keys, or order endpoints.",
        "",
        "## Current Targets",
        "",
        "| Rank | Category | Ticker | Score | Volume | Open Interest | Spread | Deadline Days | Query Terms |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    if not markets:
        lines.append("| n/a | n/a | n/a | 0 | 0 | 0 | n/a | n/a | n/a |")
    for idx, item in enumerate(markets, start=1):
        record = item["record"]
        lines.append(
            f"| {idx} | {item.get('category')} | `{record.get('ticker')}` | {_fmt(item.get('score'))} | "
            f"{_fmt(item.get('volume'))} | {_fmt(item.get('open_interest'))} | {_fmt(item.get('spread'))} | "
            f"{_fmt(item.get('deadline_days'))} | {', '.join(item.get('query_terms') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Run Notes",
            "",
            "- This list is for research monitoring only, not trading.",
            "- Kalshi volume/open-interest fields are venue-specific and should not be compared one-for-one with Polymarket liquidity.",
            "- Cross-venue matching, orderbook depth checks, and live micro capture are separate follow-up work.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _diversified_kalshi_markets(markets: Sequence[Mapping[str, object]], *, max_targets: int) -> list[Mapping[str, object]]:
    selected = []
    selected_categories = set()
    for item in markets:
        category = str(item.get("category") or "other").lower()
        if category in selected_categories:
            continue
        selected.append(item)
        selected_categories.add(category)
        if len(selected) >= max_targets:
            return selected
    for item in markets:
        ticker = str((item.get("record") or {}).get("ticker") or "")
        if ticker in {str((row.get("record") or {}).get("ticker") or "") for row in selected}:
            continue
        selected.append(item)
        if len(selected) >= max_targets:
            break
    return selected


def _kalshi_text(row: Mapping[str, object]) -> str:
    fields = [
        row.get("ticker"),
        row.get("event_ticker"),
        row.get("series_ticker"),
        row.get("title"),
        row.get("subtitle"),
        row.get("yes_sub_title"),
        row.get("no_sub_title"),
        row.get("category"),
        row.get("settlement_sources"),
    ]
    return " ".join(str(item) for item in fields if item)


def _focus_allows_text(text: str, focus: str) -> bool:
    normalized = focus.lower()
    haystack = text.lower()
    terms = set(words(haystack))
    is_sports = bool(terms & {"fifa", "nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "basketball", "baseball"}) or "world cup" in haystack
    has_narrative = bool(terms & NARRATIVE_MARKERS)
    if normalized == "all":
        return True
    if normalized == "sports":
        return is_sports
    if normalized == "crypto":
        return bool(terms & {"crypto", "bitcoin", "ethereum", "solana", "binance", "coinbase", "etf", "sec"})
    if normalized in {"politics", "geopolitics"}:
        return bool(terms & {"iran", "israel", "china", "taiwan", "trump", "election", "tariff", "sanction", "war", "ceasefire", "nuclear"})
    return has_narrative and not is_sports


def _kalshi_query_terms(row: Mapping[str, object], max_terms: int = 5) -> list[str]:
    counts = Counter(
        term
        for term in words(_kalshi_text(row))
        if len(term) > 2 and not term.isdigit() and term not in DISCOVERY_STOPWORDS and term not in {"kalshi", "ticker"}
    )
    prioritized = sorted(counts, key=lambda term: ((term not in NARRATIVE_MARKERS), -counts[term], term))
    return prioritized[:max_terms]


def _target_category_from_text(text: str) -> str:
    haystack = text.lower()
    terms = set(words(haystack))
    if terms & {"bitcoin", "crypto", "ethereum", "solana", "binance", "coinbase", "etf"}:
        return "crypto"
    if terms & {"china", "taiwan", "iran", "israel", "war", "ceasefire", "nuclear", "sanction"}:
        return "geopolitics"
    if terms & {"trump", "president", "presidential", "democratic", "republican", "nomination", "election"}:
        return "us_politics"
    if terms & {"fed", "rate", "inflation", "tariff", "recession", "gdp", "cpi"}:
        return "macro_policy"
    if terms & {"fifa", "nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "basketball", "baseball"} or "world cup" in haystack:
        return "sports"
    return "other"


def _kalshi_float(row: Mapping[str, object], *names: str) -> float:
    for name in names:
        value = row.get(name)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _kalshi_spread(row: Mapping[str, object]) -> Optional[float]:
    yes_bid = _kalshi_float(row, "yes_bid_dollars")
    yes_ask = _kalshi_float(row, "yes_ask_dollars")
    if yes_bid <= 0 or yes_ask <= 0 or yes_ask < yes_bid:
        return None
    return round(yes_ask - yes_bid, 4)


def _kalshi_deadline_days(row: Mapping[str, object], now: datetime) -> Optional[float]:
    raw = row.get("close_time") or row.get("latest_expiration_time")
    dt = to_datetime(str(raw)) if raw else None
    if dt is None:
        return None
    return round((dt - now).total_seconds() / 86400, 3)


class RedditPublicClient:
    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "signal-foundry-research-os/0.1"})

    def search(self, query: str, limit: int = 5) -> list[dict]:
        if not query:
            return []
        response = self.session.get(
            "https://www.reddit.com/search.json",
            params={"q": query, "sort": "new", "t": "day", "limit": max(1, min(limit, 25))},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        rows = []
        for child in ((payload.get("data") or {}).get("children") or []):
            data = child.get("data") if isinstance(child, dict) else None
            if not isinstance(data, dict):
                continue
            created = data.get("created_utc")
            rows.append(
                {
                    "platform": "reddit",
                    "post_id": data.get("id"),
                    "handle": data.get("author"),
                    "created_at": datetime.fromtimestamp(float(created), timezone.utc).isoformat() if created else utc_now_iso(),
                    "text": f"{data.get('title') or ''} {data.get('selftext') or ''}".strip(),
                    "score": data.get("score"),
                    "comments": data.get("num_comments"),
                    "subreddit": data.get("subreddit"),
                    "url": f"https://www.reddit.com{data.get('permalink')}" if data.get("permalink") else data.get("url"),
                    "raw_json": data,
                }
            )
        return rows


def discovery_run_id() -> str:
    return stable_hash(["signal-discovery", datetime.now(timezone.utc).strftime("%Y%m%d%H")])[:16]


def planned_x_calls_for_discovery(max_markets: int) -> int:
    return max(0, int(max_markets))


def _is_open_market(raw: Mapping[str, object], record: MarketRecord, now: datetime) -> bool:
    closed = raw.get("closed")
    active = raw.get("active")
    archived = raw.get("archived")
    if closed is True or archived is True:
        return False
    if active is False:
        return False
    end = to_datetime(record.end_time)
    if end is not None and end <= now:
        return False
    return True


def _deadline_days(record: MarketRecord, now: datetime) -> Optional[float]:
    end = to_datetime(record.end_time)
    if end is None:
        return None
    return max(0.0, (end - now).total_seconds() / 86400)


def _market_volume(raw: Mapping[str, object]) -> float:
    return float(first_float(raw, "volume", "volumeNum", "volume24hr", "volume1wk", "volume1mo") or 0)


def _public_metrics(post: Mapping[str, object]) -> Mapping[str, int]:
    metrics = post.get("public_metrics")
    if isinstance(metrics, Mapping):
        return {
            "like_count": int(metrics.get("like_count") or 0),
            "retweet_count": int(metrics.get("retweet_count") or 0),
            "reply_count": int(metrics.get("reply_count") or 0),
        }
    return {"like_count": 0, "retweet_count": 0, "reply_count": 0}


def _fmt(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)


def _source_label(row: Mapping[str, object]) -> str:
    handle = str(row.get("handle") or "").strip()
    if not handle:
        return "n/a"
    return handle if handle.startswith("r/") else f"@{handle}" if row.get("platform") == "x" else handle
