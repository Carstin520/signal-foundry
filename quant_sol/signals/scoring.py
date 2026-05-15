from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import duckdb

from .config import FomoModelConfig, SocialHandle, load_fomo_config, parse_duration
from .matching import match_posts_to_markets
from .models import SignalScore
from .storage import (
    active_markets,
    social_posts_since,
    upsert_market_fomo_states,
    upsert_mentions,
    upsert_narrative_snapshots,
    upsert_signal_outcomes,
)
from .utils import stable_hash, to_datetime, utc_now_iso


CONFIRMATION_CATEGORY = "confirmation_sources"


def score_recent(
    con: duckdb.DuckDBPyConnection,
    since_iso: str,
    handles: Sequence[SocialHandle],
    rules,
    fomo_config: Optional[FomoModelConfig] = None,
) -> List[SignalScore]:
    """Score market-level FOMO divergence instead of post-level news reaction."""
    config = fomo_config or load_fomo_config()
    posts = social_posts_since(con, since_iso)
    markets = active_markets(con)
    mentions = match_posts_to_markets(posts, markets, rules)
    upsert_mentions(con, mentions)

    post_by_id = {str(post["post_id"]): post for post in posts}
    market_by_slug = {str(market["market_slug"]): market for market in markets}
    handle_meta = {handle.handle.lower(): handle for handle in handles}
    grouped: Dict[str, list] = defaultdict(list)
    for mention in mentions:
        if mention.market_slug in market_by_slug and mention.post_id in post_by_id:
            grouped[mention.market_slug].append((mention, post_by_id[mention.post_id]))

    signals: List[SignalScore] = []
    narrative_snapshots: List[dict] = []
    fomo_states: List[dict] = []
    for market_slug, items in grouped.items():
        signal, snapshots, state = score_market_fomo(
            con,
            market_by_slug[market_slug],
            items,
            handle_meta,
            config,
        )
        narrative_snapshots.extend(snapshots)
        fomo_states.append(state)
        signals.append(signal)

    upsert_narrative_snapshots(con, narrative_snapshots)
    upsert_market_fomo_states(con, fomo_states)
    return signals


def score_market_fomo(
    con: duckdb.DuckDBPyConnection,
    market: Mapping[str, object],
    mention_post_pairs: Sequence[Tuple[object, Mapping[str, object]]],
    handle_meta: Mapping[str, SocialHandle],
    config: FomoModelConfig,
) -> Tuple[SignalScore, List[dict], dict]:
    snapshot_at = _snapshot_at(mention_post_pairs)
    market_slug = str(market.get("market_slug") or "")
    event_family = str(market.get("event_slug") or market_slug)
    narrative_items = [
        (mention, post)
        for mention, post in mention_post_pairs
        if _handle_category(post, handle_meta) != CONFIRMATION_CATEGORY
    ]
    confirmation_items = [
        (mention, post)
        for mention, post in mention_post_pairs
        if _handle_category(post, handle_meta) == CONFIRMATION_CATEGORY
    ]
    direction, direction_hits = _direction(mention_post_pairs, config)
    state = _market_state(con, market, snapshot_at, config)
    snapshots = _narrative_snapshots(market, narrative_items, handle_meta, snapshot_at, config, direction, direction_hits)

    source_quality_score = _source_quality_score(narrative_items, handle_meta)
    social_velocity_score = _social_velocity_score(snapshots, config)
    narrative_acceleration_score = _narrative_acceleration_score(narrative_items, handle_meta, snapshot_at, config)
    market_inertia_score = _market_inertia_score(state, config)
    fomo_capacity_score = int(round(float(state.get("fomo_capacity") or 0)))
    liquidity_executability_score = _liquidity_executability_score(state, config)
    wallet_flows = _wallet_flows(con, [market_slug, event_family], snapshot_at, direction)
    early_wallet_confirmation_score = min(10, int(sum(abs(float(flow.get("notional") or 0)) for flow in wallet_flows) / 25_000))
    risk_tags = _risk_tags(state, direction, narrative_items, confirmation_items, handle_meta, config)
    anti_front_run_penalty = _anti_front_run_penalty(risk_tags)

    raw_score = (
        source_quality_score
        + social_velocity_score
        + narrative_acceleration_score
        + market_inertia_score
        + fomo_capacity_score
        + liquidity_executability_score
        + early_wallet_confirmation_score
        - anti_front_run_penalty
    )
    score = max(0, min(100, raw_score))
    direction_hint = "yes_up" if direction == "bullish" else "yes_down" if direction == "bearish" else "watch_only"
    confidence = "high" if score >= 85 else "medium" if score >= 65 else "low"
    signal_id = stable_hash([market_slug, direction_hint, snapshot_at.isoformat()[:13]])[:24]

    evidence = {
        "source_quality_score": source_quality_score,
        "social_velocity_score": social_velocity_score,
        "narrative_acceleration_score": narrative_acceleration_score,
        "market_inertia_score": market_inertia_score,
        "fomo_capacity_score": fomo_capacity_score,
        "liquidity_executability_score": liquidity_executability_score,
        "early_wallet_confirmation_score": early_wallet_confirmation_score,
        "anti_front_run_penalty": anti_front_run_penalty,
        "direction_hits": direction_hits,
        "post_count_1h": _snapshot_value(snapshots, "1h", "post_count"),
        "post_count_6h": _snapshot_value(snapshots, "6h", "post_count"),
        "post_count_24h": _snapshot_value(snapshots, "24h", "post_count"),
    }
    price_window = {
        "current_market_probability": state.get("mid"),
        "narrative_direction": direction,
        "narrative_velocity": social_velocity_score,
        "market_move_1h": state.get("move_1h"),
        "market_move_6h": state.get("move_6h"),
        "market_move_24h": state.get("move_24h"),
        "spread": state.get("spread"),
        "liquidity": state.get("liquidity"),
        "deadline_days": state.get("deadline_days"),
        "fomo_capacity": state.get("fomo_capacity"),
        "confirmation_status": "confirmed_or_officially_annotated" if confirmation_items else "unconfirmed",
        "price_band": state.get("price_band"),
    }

    return (
        SignalScore(
            signal_id=signal_id,
            event_family=event_family,
            market_slug=market_slug,
            direction_hint=direction_hint,
            score=score,
            confidence=confidence,
            evidence=evidence,
            risk_tags=risk_tags,
            source_posts=_source_posts(mention_post_pairs),
            wallet_flows=wallet_flows,
            price_window=price_window,
        ),
        snapshots,
        state,
    )


def should_alert(signal: SignalScore, threshold: Optional[int] = None) -> bool:
    config = load_fomo_config()
    threshold_value = threshold if threshold is not None else config.alert_threshold
    blocked = {
        "confirmed_news",
        "near_deadline_rejected",
        "already_priced_in",
        "not_executable",
        "watch_only",
        "crowded_trade",
        "lottery_tail",
        "low_liquidity_pump",
    }
    if signal.score < threshold_value or blocked.intersection(signal.risk_tags):
        return False
    evidence = signal.evidence
    return (
        int(evidence.get("social_velocity_score") or 0) >= 10
        and int(evidence.get("market_inertia_score") or 0) >= 8
        and int(evidence.get("fomo_capacity_score") or 0) >= 8
    )


def evaluate_signal_outcomes(
    con: duckdb.DuckDBPyConnection,
    horizon: str,
    fomo_config: Optional[FomoModelConfig] = None,
) -> List[dict]:
    config = fomo_config or load_fomo_config()
    seconds = parse_duration(horizon)
    rows = con.execute(
        """
        select signal_id, generated_at, market_slug, direction_hint, price_window
        from signal_events
        where signal_id not in (select signal_id from signal_outcomes where horizon = ?)
        """,
        [horizon],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    outcomes: List[dict] = []
    for row in rows:
        item = dict(zip(columns, row))
        generated_at = to_datetime(item["generated_at"])
        if generated_at is None:
            continue
        end_at = generated_at + timedelta(seconds=seconds)
        ticks = _ticks_between(con, item["market_slug"], generated_at, end_at)
        if not ticks:
            continue
        price_window = _loads(item.get("price_window"), {})
        entry_mid = _coerce_float(price_window.get("current_market_probability"))
        if entry_mid is None:
            entry_mid = _coerce_float(ticks[0].get("mid"))
        future_mid = _coerce_float(ticks[-1].get("mid"))
        if entry_mid is None or future_mid is None:
            continue
        direction_sign = 1 if item["direction_hint"] == "yes_up" else -1 if item["direction_hint"] == "yes_down" else 0
        signed_moves = [(float(tick["mid"]) - entry_mid) * direction_sign for tick in ticks if tick.get("mid") is not None]
        if not signed_moves or direction_sign == 0:
            continue
        max_favorable = max(signed_moves)
        max_adverse = min(signed_moves)
        delta = (future_mid - entry_mid) * direction_sign
        outcomes.append(
            {
                "signal_id": item["signal_id"],
                "horizon": horizon,
                "entry_mid": entry_mid,
                "future_mid": future_mid,
                "delta": delta,
                "max_favorable_delta": max_favorable,
                "max_adverse_delta": max_adverse,
                "overshoot": max_favorable >= config.strong_favorable,
                "evaluated_at": utc_now_iso(),
            }
        )
    upsert_signal_outcomes(con, outcomes)
    return outcomes


def _snapshot_at(items: Sequence[Tuple[object, Mapping[str, object]]]) -> datetime:
    timestamps = [to_datetime(post.get("created_at")) for _, post in items]
    present = [timestamp for timestamp in timestamps if timestamp is not None]
    if present:
        return max(present)
    fallback = to_datetime(utc_now_iso())
    assert fallback is not None
    return fallback


def _narrative_snapshots(
    market: Mapping[str, object],
    items: Sequence[Tuple[object, Mapping[str, object]]],
    handle_meta: Mapping[str, SocialHandle],
    snapshot_at: datetime,
    config: FomoModelConfig,
    direction: str,
    direction_hits: Mapping[str, int],
) -> List[dict]:
    windows = [config.short_window, config.medium_window, config.base_window]
    snapshots = []
    for window in windows:
        seconds = parse_duration(window)
        start = snapshot_at - timedelta(seconds=seconds)
        window_items = [(mention, post) for mention, post in items if _post_dt(post) and _post_dt(post) >= start]
        handles = {str(post.get("handle") or "").lower() for _, post in window_items}
        categories = sorted({_handle_category(post, handle_meta) for _, post in window_items if _handle_category(post, handle_meta)})
        keywords = Counter()
        weighted = 0.0
        for mention, post in window_items:
            keywords.update(getattr(mention, "keywords", []) or [])
            meta = handle_meta.get(str(post.get("handle") or "").lower())
            weighted += max(1.0, (meta.source_score if meta else 10) / 10.0)
        snapshots.append(
            {
                "snapshot_at": snapshot_at.isoformat(),
                "event_family": str(market.get("event_slug") or market.get("market_slug") or ""),
                "market_slug": str(market.get("market_slug") or ""),
                "window": window,
                "post_count": len(window_items),
                "weighted_post_count": weighted,
                "unique_handles": len(handles),
                "source_categories": categories,
                "top_keywords": [keyword for keyword, _ in keywords.most_common(10)] or list(direction_hits.keys()),
                "direction": direction,
                "sentiment_strength": min(1.0, len(window_items) / 10.0),
            }
        )
    return snapshots


def _source_quality_score(items: Sequence[Tuple[object, Mapping[str, object]]], handle_meta: Mapping[str, SocialHandle]) -> int:
    if not items:
        return 0
    seen = {}
    for _, post in items:
        handle = str(post.get("handle") or "").lower()
        meta = handle_meta.get(handle)
        if meta and meta.category != CONFIRMATION_CATEGORY:
            seen[handle] = max(seen.get(handle, 0), meta.source_score)
    return min(20, int(sum(seen.values()) / 2))


def _social_velocity_score(snapshots: Sequence[Mapping[str, object]], config: FomoModelConfig) -> int:
    by_window = {snapshot["window"]: snapshot for snapshot in snapshots}
    one_hour = float(by_window.get(config.short_window, {}).get("post_count") or 0)
    six_hour = float(by_window.get(config.medium_window, {}).get("post_count") or 0)
    day = float(by_window.get(config.base_window, {}).get("post_count") or 0)
    velocity = six_hour * 4
    if one_hour > max(1.0, six_hour / 6):
        velocity += (one_hour - max(1.0, six_hour / 6)) * 5
    if day and six_hour / max(day, 1.0) >= 0.5:
        velocity += 4
    return min(20, int(round(velocity)))


def _narrative_acceleration_score(
    items: Sequence[Tuple[object, Mapping[str, object]]],
    handle_meta: Mapping[str, SocialHandle],
    snapshot_at: datetime,
    config: FomoModelConfig,
) -> int:
    if len(items) < 2:
        return 0
    medium_start = snapshot_at - timedelta(seconds=parse_duration(config.medium_window))
    recent = [(mention, post) for mention, post in items if _post_dt(post) and _post_dt(post) >= medium_start]
    unique_handles = {str(post.get("handle") or "").lower() for _, post in recent}
    categories = {
        _handle_category(post, handle_meta)
        for _, post in recent
        if _handle_category(post, handle_meta) and _handle_category(post, handle_meta) != CONFIRMATION_CATEGORY
    }
    return min(15, max(0, (len(unique_handles) - 1) * 3 + len(categories) * 4))


def _market_state(
    con: duckdb.DuckDBPyConnection,
    market: Mapping[str, object],
    snapshot_at: datetime,
    config: FomoModelConfig,
) -> dict:
    market_slug = str(market.get("market_slug") or "")
    current = _latest_tick_before(con, market_slug, snapshot_at)
    mid = _coerce_float(current.get("mid") if current else None)
    spread = _coerce_float(current.get("spread") if current else None)
    liquidity = _coerce_float((current or {}).get("liquidity")) or _coerce_float(market.get("liquidity"))
    move_1h = _move_since(con, market_slug, snapshot_at, mid, hours=1)
    move_6h = _move_since(con, market_slug, snapshot_at, mid, hours=6)
    move_24h = _move_since(con, market_slug, snapshot_at, mid, hours=24)
    deadline_days = _deadline_days(market, snapshot_at)
    price_band, fomo_capacity = _price_band(mid, config)
    return {
        "snapshot_at": snapshot_at.isoformat(),
        "market_slug": market_slug,
        "mid": mid,
        "spread": spread,
        "liquidity": liquidity,
        "price_band": price_band,
        "move_1h": move_1h,
        "move_6h": move_6h,
        "move_24h": move_24h,
        "deadline_days": deadline_days,
        "fomo_capacity": fomo_capacity,
    }


def _market_inertia_score(state: Mapping[str, object], config: FomoModelConfig) -> int:
    mid = state.get("mid")
    if mid is None:
        return 0
    move_6h = abs(float(state.get("move_6h") or 0))
    move_24h = abs(float(state.get("move_24h") or 0))
    if move_6h >= config.already_moved_6h or move_24h >= config.already_moved_24h:
        return 0
    if move_6h <= 0.02 and move_24h <= 0.05:
        return 15
    if move_6h <= 0.05 and move_24h <= 0.10:
        return 10
    return 5


def _liquidity_executability_score(state: Mapping[str, object], config: FomoModelConfig) -> int:
    liquidity = state.get("liquidity")
    spread = state.get("spread")
    if liquidity is None:
        return 4
    if float(liquidity) < config.minimum_liquidity:
        return 0
    if spread is not None and float(spread) > config.max_spread:
        return 4
    return 15


def _risk_tags(
    state: Mapping[str, object],
    direction: str,
    narrative_items: Sequence[Tuple[object, Mapping[str, object]]],
    confirmation_items: Sequence[Tuple[object, Mapping[str, object]]],
    handle_meta: Mapping[str, SocialHandle],
    config: FomoModelConfig,
) -> List[str]:
    tags: List[str] = []
    if confirmation_items:
        tags.append("confirmed_news")
    if direction == "watch_only":
        tags.append("watch_only")
    if state.get("mid") is None:
        tags.append("no_market_price")
    if state.get("price_band") in {"lottery_tail", "crowded"}:
        tags.append("lottery_tail" if state.get("price_band") == "lottery_tail" else "crowded_trade")
    deadline_days = state.get("deadline_days")
    if deadline_days is not None:
        deadline_hours = float(deadline_days) * 24
        if 0 <= deadline_hours < config.hard_deadline_hours:
            tags.append("near_deadline_rejected")
        elif 0 <= float(deadline_days) < config.soft_deadline_days:
            tags.append("near_deadline")
    if abs(float(state.get("move_6h") or 0)) >= config.already_moved_6h or abs(float(state.get("move_24h") or 0)) >= config.already_moved_24h:
        tags.append("already_priced_in")
    if state.get("liquidity") is not None and float(state["liquidity"]) < config.minimum_liquidity:
        tags.append("not_executable")
    if state.get("spread") is not None and float(state["spread"]) > config.max_spread:
        tags.append("not_executable")
    handles = {str(post.get("handle") or "").lower() for _, post in narrative_items}
    if len(narrative_items) >= 3 and len(handles) <= 1:
        tags.append("low_liquidity_pump")
    if len(narrative_items) < 2:
        tags.append("weak_narrative")
    return sorted(dict.fromkeys(tags))


def _anti_front_run_penalty(tags: Sequence[str]) -> int:
    weights = {
        "confirmed_news": 30,
        "near_deadline_rejected": 40,
        "near_deadline": 15,
        "already_priced_in": 25,
        "not_executable": 18,
        "crowded_trade": 12,
        "lottery_tail": 10,
        "watch_only": 12,
        "weak_narrative": 8,
        "no_market_price": 10,
        "low_liquidity_pump": 15,
    }
    return min(60, sum(weights.get(tag, 0) for tag in tags))


def _direction(items: Sequence[Tuple[object, Mapping[str, object]]], config: FomoModelConfig) -> Tuple[str, Dict[str, int]]:
    texts = " ".join(str(post.get("text") or "").lower() for _, post in items)
    bullish = {keyword: texts.count(keyword) for keyword in config.bullish_keywords if keyword in texts}
    bearish = {keyword: texts.count(keyword) for keyword in config.bearish_keywords if keyword in texts}
    bull_score = sum(bullish.values())
    bear_score = sum(bearish.values())
    if bull_score > bear_score:
        return "bullish", bullish
    if bear_score > bull_score:
        return "bearish", bearish
    return "watch_only", {}


def _wallet_flows(con: duckdb.DuckDBPyConnection, market_keys: Sequence[str], snapshot_at: datetime, direction: str) -> List[dict]:
    start = (snapshot_at - timedelta(hours=24)).isoformat()
    end = snapshot_at.isoformat()
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


def _latest_tick_before(con: duckdb.DuckDBPyConnection, market_slug: str, at: datetime) -> Optional[dict]:
    row = con.execute(
        """
        select observed_at, mid, spread, liquidity, best_bid, best_ask, last_trade_price
        from market_ticks
        where market_slug = ? and observed_at <= ?
        order by observed_at desc
        limit 1
        """,
        [market_slug, at.isoformat()],
    ).fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in con.description]
    return dict(zip(columns, row))


def _move_since(con: duckdb.DuckDBPyConnection, market_slug: str, snapshot_at: datetime, current_mid: Optional[float], hours: int) -> Optional[float]:
    if current_mid is None:
        return None
    baseline_at = snapshot_at - timedelta(hours=hours)
    baseline = _latest_tick_before(con, market_slug, baseline_at)
    baseline_mid = _coerce_float(baseline.get("mid") if baseline else None)
    if baseline_mid is None:
        return 0.0
    return current_mid - baseline_mid


def _ticks_between(con: duckdb.DuckDBPyConnection, market_slug: str, start: datetime, end: datetime) -> List[dict]:
    rows = con.execute(
        """
        select observed_at, mid
        from market_ticks
        where market_slug = ? and observed_at between ? and ?
        order by observed_at
        """,
        [market_slug, start.isoformat(), end.isoformat()],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _price_band(mid: Optional[float], config: FomoModelConfig) -> Tuple[str, int]:
    if mid is None:
        return "unknown", 0
    if mid <= config.lottery_tail_max:
        return "lottery_tail", 0
    if config.ideal_price_min <= mid <= config.ideal_price_max:
        return "ideal", 20
    if config.acceptable_price_min <= mid <= config.acceptable_price_max:
        return "acceptable", 12
    if mid >= config.crowded_min:
        return "crowded", 0
    return "weak", 6


def _deadline_days(market: Mapping[str, object], snapshot_at: datetime) -> Optional[float]:
    end = to_datetime(market.get("end_time"))
    if end is None:
        return None
    return (end - snapshot_at).total_seconds() / 86400


def _source_posts(items: Sequence[Tuple[object, Mapping[str, object]]]) -> List[dict]:
    posts = []
    for _, post in sorted(items, key=lambda pair: str(pair[1].get("created_at") or ""), reverse=True)[:8]:
        posts.append(
            {
                "handle": post.get("handle"),
                "created_at": str(post.get("created_at")),
                "text": post.get("text"),
                "url": post.get("url"),
            }
        )
    return posts


def _snapshot_value(snapshots: Sequence[Mapping[str, object]], window: str, key: str) -> object:
    for snapshot in snapshots:
        if snapshot.get("window") == window:
            return snapshot.get(key)
    return None


def _post_dt(post: Mapping[str, object]) -> Optional[datetime]:
    return to_datetime(post.get("created_at"))


def _handle_category(post: Mapping[str, object], handle_meta: Mapping[str, SocialHandle]) -> str:
    handle = str(post.get("handle") or "").lower()
    meta = handle_meta.get(handle)
    return meta.category if meta else "unknown"


def _coerce_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _loads(value: object, default):
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
