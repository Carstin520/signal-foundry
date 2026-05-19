from __future__ import annotations

import json
from datetime import timedelta
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import duckdb

from .config import SocialHandle
from .matching import match_posts_to_markets
from .models import SignalScore
from .storage import active_markets, social_posts_since, upsert_mentions
from .utils import stable_hash, to_datetime, utc_now_iso


def score_recent(
    con: duckdb.DuckDBPyConnection,
    since_iso: str,
    handles: Sequence[SocialHandle],
    rules,
) -> List[SignalScore]:
    posts = social_posts_since(con, since_iso)
    markets = active_markets(con)
    mentions = match_posts_to_markets(posts, markets, rules)
    upsert_mentions(con, mentions)

    handle_scores = {handle.handle.lower(): handle.source_score for handle in handles}
    post_by_id = {str(post["post_id"]): post for post in posts}
    market_by_slug = {str(market["market_slug"]): market for market in markets}

    signals: List[SignalScore] = []
    for mention in mentions:
        post = post_by_id.get(mention.post_id)
        market = market_by_slug.get(mention.market_slug)
        if not post or not market:
            continue
        signals.append(score_mention(con, post, market, mention, handle_scores))
    return signals


def score_mention(
    con: duckdb.DuckDBPyConnection,
    post: Mapping[str, object],
    market: Mapping[str, object],
    mention,
    handle_scores: Mapping[str, int],
) -> SignalScore:
    post_ts = to_datetime(post.get("created_at"))
    if post_ts is None:
        post_ts = to_datetime(utc_now_iso())
    assert post_ts is not None

    source_score = int(handle_scores.get(str(post.get("handle") or "").lower(), 10))
    match_score = int(round(min(max(float(mention.confidence), 0.0), 1.0) * 25))
    price_window = _price_window(con, mention.market_slug, post_ts)
    price_score = _price_score(price_window)
    wallet_flows = _wallet_flows(con, [mention.market_slug, str(market.get("event_slug") or "")], post_ts)
    wallet_score = min(15, int(sum(abs(float(flow.get("notional") or 0)) for flow in wallet_flows) / 10_000))
    risk_tags = _risk_tags(market, price_window, mention.confidence)
    risk_penalty = _risk_penalty(risk_tags)
    raw_score = source_score + match_score + price_score + wallet_score - risk_penalty
    score = max(0, min(100, raw_score))

    delta = _best_delta(price_window)
    direction_hint = "yes_up" if delta and delta > 0 else "yes_down" if delta and delta < 0 else "unknown"
    confidence = "high" if score >= 80 else "medium" if score >= 60 else "low"
    signal_id = stable_hash([post.get("post_id"), mention.market_slug, int(score)])[:24]

    return SignalScore(
        signal_id=signal_id,
        event_family=str(market.get("event_slug") or mention.market_slug),
        market_slug=mention.market_slug,
        direction_hint=direction_hint,
        score=score,
        confidence=confidence,
        evidence={
            "source_score": source_score,
            "market_match_score": match_score,
            "price_reaction_score": price_score,
            "wallet_flow_score": wallet_score,
            "risk_penalty": risk_penalty,
            "matched_entities": mention.entities,
            "matched_keywords": mention.keywords,
        },
        risk_tags=risk_tags,
        source_posts=[
            {
                "handle": post.get("handle"),
                "created_at": str(post.get("created_at")),
                "text": post.get("text"),
                "url": post.get("url"),
            }
        ],
        wallet_flows=wallet_flows,
        price_window=price_window,
    )


def should_alert(signal: SignalScore, threshold: int = 70) -> bool:
    evidence = signal.evidence
    components = [
        int(evidence.get("source_score") or 0) >= 20,
        int(evidence.get("market_match_score") or 0) >= 10,
        int(evidence.get("price_reaction_score") or 0) >= 10,
    ]
    return signal.score >= threshold and sum(components) >= 2


def _price_window(con: duckdb.DuckDBPyConnection, market_slug: str, post_ts) -> Dict[str, object]:
    before = _tick_near(con, market_slug, post_ts - timedelta(minutes=5), post_ts)
    after_1m = _tick_near(con, market_slug, post_ts, post_ts + timedelta(minutes=1))
    after_5m = _tick_near(con, market_slug, post_ts, post_ts + timedelta(minutes=5))
    after_15m = _tick_near(con, market_slug, post_ts, post_ts + timedelta(minutes=15))
    after_60m = _tick_near(con, market_slug, post_ts, post_ts + timedelta(minutes=60))
    base = _mid(before)
    return {
        "pre_5m_mid": base,
        "post_1m_mid": _mid(after_1m),
        "post_5m_mid": _mid(after_5m),
        "post_15m_mid": _mid(after_15m),
        "post_60m_mid": _mid(after_60m),
        "spread": _latest_field(after_1m, after_5m, after_15m, field="spread"),
        "liquidity": _latest_field(after_1m, after_5m, after_15m, field="liquidity"),
        "delta_1m": _delta(base, _mid(after_1m)),
        "delta_5m": _delta(base, _mid(after_5m)),
        "delta_15m": _delta(base, _mid(after_15m)),
        "delta_60m": _delta(base, _mid(after_60m)),
    }


def _tick_near(con: duckdb.DuckDBPyConnection, market_slug: str, start, end) -> Optional[dict]:
    row = con.execute(
        """
        select observed_at, mid, spread, liquidity, best_bid, best_ask, last_trade_price
        from market_ticks
        where market_slug = ? and observed_at between ? and ?
        order by observed_at desc
        limit 1
        """,
        [market_slug, start.isoformat(), end.isoformat()],
    ).fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in con.description]
    return dict(zip(columns, row))


def _wallet_flows(con: duckdb.DuckDBPyConnection, market_keys: Sequence[str], post_ts) -> List[dict]:
    start = (post_ts - timedelta(minutes=60)).isoformat()
    end = (post_ts + timedelta(minutes=60)).isoformat()
    keys = [key for key in dict.fromkeys(market_keys) if key]
    if not keys:
        return []
    placeholders = ", ".join(["?"] * len(keys))
    rows = con.execute(
        f"""
        select wallet, side, price, size, notional, activity_ts, tx_hash
        from wallet_activity
        where market_slug in ({placeholders}) and activity_ts between ? and ?
        order by abs(coalesce(notional, 0)) desc
        limit 10
        """,
        [*keys, start, end],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    flows = []
    for row in rows:
        item = dict(zip(columns, row))
        item["activity_ts"] = str(item.get("activity_ts"))
        flows.append(item)
    return flows


def _risk_tags(market: Mapping[str, object], price_window: Mapping[str, object], confidence: float) -> List[str]:
    tags: List[str] = []
    spread = price_window.get("spread")
    liquidity = price_window.get("liquidity")
    if price_window.get("pre_5m_mid") is None:
        tags.append("social_only_no_price_move")
    if spread is not None and float(spread) >= 0.05:
        tags.append("wide_spread")
    if liquidity is not None and float(liquidity) < 10_000:
        tags.append("thin_liquidity")
    if confidence < 0.4:
        tags.append("weak_market_match")
    end_time = to_datetime(market.get("end_time"))
    if end_time is not None:
        now = to_datetime(utc_now_iso())
        if now is not None and timedelta(0) <= end_time - now <= timedelta(days=1):
            tags.append("late_resolution")
    return tags


def _risk_penalty(tags: Sequence[str]) -> int:
    weights = {
        "social_only_no_price_move": 8,
        "wide_spread": 8,
        "thin_liquidity": 8,
        "weak_market_match": 6,
        "late_resolution": 6,
    }
    return min(25, sum(weights.get(tag, 0) for tag in tags))


def _price_score(price_window: Mapping[str, object]) -> int:
    delta = _best_delta(price_window)
    if delta is None:
        return 0
    return min(25, int(abs(delta) * 250))


def _best_delta(price_window: Mapping[str, object]) -> Optional[float]:
    values = [price_window.get("delta_5m"), price_window.get("delta_15m"), price_window.get("delta_60m"), price_window.get("delta_1m")]
    present = [float(value) for value in values if value is not None]
    if not present:
        return None
    return max(present, key=abs)


def _mid(tick: Optional[Mapping[str, object]]) -> Optional[float]:
    if not tick or tick.get("mid") is None:
        return None
    return float(tick["mid"])


def _delta(base: Optional[float], value: Optional[float]) -> Optional[float]:
    if base is None or value is None:
        return None
    return value - base


def _latest_field(*ticks: Optional[Mapping[str, object]], field: str) -> Optional[float]:
    for tick in ticks:
        if tick and tick.get(field) is not None:
            return float(tick[field])
    return None
