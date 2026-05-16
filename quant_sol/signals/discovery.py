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

from .clients import GammaMarketClient, XApiClient, market_record_from_gamma
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
    dry_run: bool = False,
) -> dict:
    markets = discover_interesting_markets(
        max_markets=max_markets,
        max_gamma_pages=max_gamma_pages,
        min_liquidity=min_liquidity,
        focus=focus,
    )
    if dry_run:
        return {
            "markets": markets,
            "x_posts": [],
            "source_candidates": [],
            "reddit_posts": [],
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


def x_query_for_market(record: MarketRecord) -> str:
    terms = market_query_terms(record, max_terms=4)
    if not terms:
        return ""
    query = " ".join(terms)
    return f"({query}) -is:retweet"


def reddit_query_for_market(record: MarketRecord) -> str:
    return " ".join(market_query_terms(record, max_terms=4))


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
                "evidence": {
                    "post_ids": sorted(unique_posts)[:10],
                    "reddit_context_posts": reddit_context,
                    "market_score": market_scores.get(market_slug),
                },
            }
        )
    return sorted(rows, key=lambda row: row["discovery_score"], reverse=True)


def write_signal_discovery_report(result: Mapping[str, object], report_root: Path) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    path = report_root / f"signal_source_discovery_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
    markets = list(result.get("markets") or [])
    sources = list(result.get("source_candidates") or [])
    reddit_posts = list(result.get("reddit_posts") or [])
    lines = [
        "# Signal Source Discovery",
        "",
        f"- Generated at: {utc_now_iso()}",
        f"- Markets scanned: {len(markets)}",
        f"- X posts collected: {len(result.get('x_posts') or [])}",
        f"- Reddit context posts collected: {len(reddit_posts)}",
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
            "## Source Candidates",
            "",
            "| Rank | Platform | Handle | Market | Score | Posts | Engagement | Status |",
            "| ---: | --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    if not sources:
        lines.append("| n/a | n/a | n/a | n/a | 0 | 0 | 0 | no_candidates |")
    for idx, row in enumerate(sources[:50], start=1):
        lines.append(
            f"| {idx} | {row.get('platform')} | @{row.get('handle')} | `{row.get('market_slug')}` | "
            f"{_fmt(row.get('discovery_score'))} | {row.get('post_count')} | {_fmt(row.get('engagement_score'))} | "
            f"{row.get('recommended_status')} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This discovery report is for candidate sourcing, not trading.",
            "- X searches are intentionally bounded by market count and posts per market.",
            "- Reddit is used as broad context/discussion discovery, not as a high-confidence signal source.",
            "- Candidates should be promoted to watchlists only after repeated post-to-price evidence.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


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
