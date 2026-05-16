from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Optional, Sequence

import duckdb

from .config import SIGNAL_REPORT_ROOT, parse_duration
from .history import case_keywords, direction_from_text, get_event_case
from .storage import (
    latest_backtest_run,
    post_price_event_matches_for_case,
    price_events_for_case,
    source_backfill_plans_for_case,
    upsert_account_backtest_metrics,
    upsert_backtest_run,
    upsert_backtest_samples,
    upsert_post_price_event_matches,
    upsert_price_events,
    upsert_source_backfill_plans,
)
from .utils import stable_hash, to_datetime, utc_now_iso, words


DEFAULT_PRICE_WINDOWS = ("10m", "30m", "2h")
DEFAULT_PRICE_FIRST_HORIZONS = ("1m", "5m", "10m", "30m", "2h")
DEFAULT_SOURCE_PRE = "6h"
DEFAULT_SOURCE_POST = "30m"
PRICE_EVENT_MIN_TICKS = 3
PAPER_MIN_EDGE = 0.03
PAPER_MAX_ADVERSE = 0.04
PAPER_MIN_REWARD_TO_RISK = 1.5
PAPER_MIN_ROUND_TRIP_COST = 0.01
PAPER_SLIPPAGE_BUFFER = 0.002
MIN_SOURCE_SAMPLES = 3
FULL_CONFIDENCE_SAMPLES = 5


CountProvider = Callable[[str, str, str], Mapping[str, object]]


def mine_price_events(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    windows: Sequence[str] = DEFAULT_PRICE_WINDOWS,
    min_move_pp: float = 3.0,
) -> list[dict]:
    case = get_event_case(con, case_id)
    if not case or not case.get("market_slug"):
        return []
    market_slug = str(case["market_slug"])
    token_id = _yes_token_id(con, market_slug)
    ticks = _market_ticks(con, market_slug, token_id)
    threshold = float(min_move_pp) / 100.0
    candidates: list[dict] = []
    config_hash = stable_hash({"windows": list(windows), "min_move_pp": min_move_pp})[:16]
    for window in windows:
        window_seconds = parse_duration(window)
        for index, start_tick in enumerate(ticks):
            start_ts = to_datetime(start_tick.get("observed_at"))
            if start_ts is None or start_tick.get("mid") is None:
                continue
            end_ts = start_ts + timedelta(seconds=window_seconds)
            window_ticks = [tick for tick in ticks[index:] if _between(tick, start_ts, end_ts)]
            if len(window_ticks) < 2:
                continue
            event = _price_event_from_window(case_id, market_slug, start_tick, window_ticks, threshold, window, config_hash)
            if event:
                candidates.append(event)
    events = _dedupe_price_events(candidates)
    upsert_price_events(con, events)
    return events


def plan_source_backfill(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    pre: str = DEFAULT_SOURCE_PRE,
    post: str = DEFAULT_SOURCE_POST,
    platform: str = "x",
    use_counts: bool = True,
    daily_cap: int = 200,
    max_count: int = 500,
    count_provider: Optional[CountProvider] = None,
    write: bool = True,
) -> list[dict]:
    case = get_event_case(con, case_id)
    if not case:
        return []
    events = price_events_for_case(con, case_id)
    pre_seconds = parse_duration(pre)
    post_seconds = parse_duration(post)
    query = _source_query_for_case(con, case)
    plans = []
    calls_used = 0
    for event in events:
        start_at = to_datetime(event.get("start_at"))
        if start_at is None:
            continue
        window_start = start_at - timedelta(seconds=pre_seconds)
        window_end = start_at + timedelta(seconds=post_seconds)
        planned_calls = 1 if use_counts else 0
        status = "planned"
        reason = None
        counts: Mapping[str, object] = {}
        if use_counts and calls_used + planned_calls > daily_cap:
            status = "cap_exceeded"
            reason = "counts_preflight_would_exceed_daily_cap"
        elif use_counts and count_provider is None:
            status = "planned_no_counts"
            reason = "no_count_provider"
            calls_used += planned_calls
        elif use_counts and count_provider is not None:
            counts = count_provider(query, _x_time(window_start), _x_time(window_end))
            calls_used += planned_calls
            total = _count_total(counts)
            if total is not None and total > max_count:
                status = "too_expensive"
                reason = f"count_{total}_exceeds_{max_count}"
        plan_id = stable_hash([case_id, event["price_event_id"], platform, query, window_start.isoformat(), window_end.isoformat()])[:24]
        plans.append(
            {
                "plan_id": plan_id,
                "case_id": case_id,
                "price_event_id": event["price_event_id"],
                "platform": platform,
                "query": query,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "planned_calls": planned_calls,
                "counts": counts,
                "status": status,
                "reason": reason,
            }
        )
    if write:
        upsert_source_backfill_plans(con, plans)
    return plans


def match_price_events(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    method: str = "keyword",
    min_confidence: float = 0.70,
    pre: str = DEFAULT_SOURCE_PRE,
    post: str = DEFAULT_SOURCE_POST,
) -> list[dict]:
    case = get_event_case(con, case_id)
    if not case:
        return []
    events = price_events_for_case(con, case_id)
    posts = _source_posts_for_case(con, case_id)
    keywords = _case_terms(con, case)
    semantic_confidence = _semantic_confidence_by_post(con, case_id) if method == "cloud" else {}
    pre_seconds = parse_duration(pre)
    post_seconds = parse_duration(post)
    rows = []
    for event in events:
        start_at = to_datetime(event.get("start_at"))
        end_at = to_datetime(event.get("end_at"))
        if start_at is None or end_at is None:
            continue
        window_start = start_at - timedelta(seconds=pre_seconds)
        window_end = start_at + timedelta(seconds=post_seconds)
        seen_handles = set()
        for post in sorted(posts, key=lambda item: str(item.get("created_at"))):
            post_ts = to_datetime(post.get("created_at"))
            if post_ts is None or not (window_start <= post_ts <= window_end):
                continue
            handle = str(post.get("handle") or "").lstrip("@")
            if not handle or handle in seen_handles:
                continue
            matched_terms = _matched_terms(str(post.get("text") or ""), keywords)
            confidence = _keyword_confidence(matched_terms)
            if method == "cloud":
                confidence = max(confidence, semantic_confidence.get(str(post.get("post_id")), 0.0))
            if confidence < min_confidence:
                continue
            direction = post.get("direction") or direction_from_text(str(post.get("text") or ""))
            row = {
                "case_id": case_id,
                "price_event_id": event["price_event_id"],
                "post_id": post["post_id"],
                "handle": handle,
                "market_slug": event["market_slug"],
                "post_created_at": post_ts.isoformat(),
                "lead_seconds": (start_at - post_ts).total_seconds(),
                "relative_position": _relative_position(post_ts, start_at, end_at),
                "match_confidence": confidence,
                "direction": direction,
                "direction_agrees": _direction_agrees(direction, str(event.get("direction") or "")),
                "method": method,
                "matched_keywords": matched_terms,
            }
            rows.append(row)
            seen_handles.add(handle)
    upsert_post_price_event_matches(con, rows)
    return rows


def run_price_first_backtest(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    horizons: Sequence[str] = DEFAULT_PRICE_FIRST_HORIZONS,
    execution: str = "top-of-book",
) -> tuple[str, list[dict], list[dict]]:
    matches = post_price_event_matches_for_case(con, case_id)
    events = {event["price_event_id"]: event for event in price_events_for_case(con, case_id)}
    run_id = upsert_backtest_run(
        con,
        {
            "case_id": case_id,
            "run_type": "price_first",
            "execution_mode": execution,
            "horizons": list(horizons),
            "config_hash": stable_hash({"horizons": list(horizons), "execution": execution})[:16],
            "notes": "historical minute-floor backtest; sub-minute conclusions are not valid",
        },
    )
    samples: list[dict] = []
    for match in matches:
        event = events.get(str(match.get("price_event_id")))
        if not event:
            continue
        ticks = _market_ticks(con, str(match["market_slug"]), event.get("token_id"))
        samples.extend(_samples_for_match(run_id, case_id, match, event, ticks, horizons))
    metrics = _account_metrics(run_id, case_id, samples)
    upsert_backtest_samples(con, samples)
    upsert_account_backtest_metrics(con, metrics)
    return run_id, samples, metrics


def write_price_first_report(con: duckdb.DuckDBPyConnection, case_id: str, report_root: Path = SIGNAL_REPORT_ROOT) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    path = report_root / f"price_first_backtest_{case_id}.md"
    case = get_event_case(con, case_id)
    events = price_events_for_case(con, case_id)
    plans = source_backfill_plans_for_case(con, case_id)
    matches = post_price_event_matches_for_case(con, case_id)
    run = latest_backtest_run(con, case_id, "price_first")
    samples = _samples_for_run(con, run["run_id"]) if run else []
    metrics = _metrics_for_run(con, run["run_id"]) if run else []
    warnings = sorted({tag for event in events for tag in (event.get("risk_tags") or [])})
    lines = [
        f"# Price-First Backtest: {case_id}",
        "",
        f"- Query: {case.get('query') if case else 'unknown'}",
        f"- Market: `{case.get('market_slug') if case else 'unknown'}`",
        "- Historical scope: minute-level validation only; 1s/10s/30s claims require live burst/WebSocket data.",
        f"- Price events: {len(events)}",
        f"- Matches: {len(matches)}",
        f"- Latest run: {run.get('run_id') if run else 'n/a'}",
        f"- Data warnings: {', '.join(warnings) if warnings else 'n/a'}",
        "",
        "## Top Price Events",
        "",
        "| Start | Type | Direction | Move | Duration | Resolution | Tags |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for event in sorted(events, key=lambda row: abs(float(row.get("move_size") or 0)), reverse=True)[:30]:
        lines.append(
            f"| {event.get('start_at')} | {event.get('event_type')} | {event.get('direction')} | "
            f"{_fmt(event.get('move_size'))} | {_fmt(event.get('duration_seconds'))} | "
            f"{event.get('price_data_resolution')} | {', '.join(event.get('risk_tags') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Account Ranking",
            "",
            "| Account | Samples | Before | During | Late | Tradable | False FOMO | EV After Cost | Confidence | Status |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    if not metrics:
        lines.append("| n/a | 0 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | no_run |")
    for row in metrics[:30]:
        lines.append(
            f"| @{row['handle']} | {row['sample_size']} | {_fmt(row.get('before_move_rate'))} | "
            f"{_fmt(row.get('during_move_rate'))} | {_fmt(row.get('late_after_price_move_rate'))} | "
            f"{_fmt(row.get('tradable_hit_rate'))} | {_fmt(row.get('false_fomo_rate'))} | "
            f"{_fmt(row.get('expectancy_after_cost'))} | {_fmt(row.get('sample_confidence'))} | "
            f"{row.get('recommended_status')} |"
        )
    lines.extend(
        [
            "",
            "## Best Pre-Move Posts",
            "",
            "| Account | Event | Lead Seconds | Confidence | Direction | Agrees |",
            "| --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for row in [match for match in matches if match.get("relative_position") == "before"][:40]:
        lines.append(
            f"| @{row['handle']} | {row['price_event_id']} | {_fmt(row.get('lead_seconds'))} | "
            f"{_fmt(row.get('match_confidence'))} | {row.get('direction')} | {row.get('direction_agrees')} |"
        )
    lines.extend(
        [
            "",
            "## Late Commentators",
            "",
            "| Account | Event | Relative | Lead Seconds | Confidence |",
            "| --- | --- | --- | ---: | ---: |",
        ]
    )
    for row in [match for match in matches if match.get("relative_position") != "before"][:40]:
        lines.append(
            f"| @{row['handle']} | {row['price_event_id']} | {row.get('relative_position')} | "
            f"{_fmt(row.get('lead_seconds'))} | {_fmt(row.get('match_confidence'))} |"
        )
    lines.extend(
        [
            "",
            "## Query Budget",
            "",
            "| Plan | Event | Status | Calls | Reason | Query |",
            "| --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for plan in plans[:50]:
        lines.append(
            f"| {plan['plan_id']} | {plan.get('price_event_id') or 'n/a'} | {plan.get('status')} | "
            f"{plan.get('planned_calls')} | {plan.get('reason') or ''} | {str(plan.get('query') or '')[:120]} |"
        )
    lines.extend(
        [
            "",
            "## Sample Checks",
            "",
            "| Account | Horizon | Relative | Entry | Future | Net | R/R | Tradable | Tags |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for sample in samples[:80]:
        lines.append(
            f"| @{sample['handle']} | {sample['horizon']} | {sample.get('relative_position')} | "
            f"{_fmt(sample.get('entry_price'))} | {_fmt(sample.get('future_price'))} | "
            f"{_fmt(sample.get('net_delta'))} | {_fmt(sample.get('reward_to_risk'))} | "
            f"{sample.get('paper_trade_positive')} | {', '.join(sample.get('risk_tags') or [])} |"
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _price_event_from_window(
    case_id: str,
    market_slug: str,
    start_tick: Mapping[str, object],
    window_ticks: Sequence[Mapping[str, object]],
    threshold: float,
    window_label: str,
    config_hash: str,
) -> Optional[dict]:
    start_ts = to_datetime(start_tick.get("observed_at"))
    start_mid = _float_value(start_tick.get("mid"))
    if start_ts is None or start_mid is None:
        return None
    deltas = [(tick, _float_value(tick.get("mid")) - start_mid) for tick in window_ticks if _float_value(tick.get("mid")) is not None]
    if not deltas:
        return None
    up_tick, up_delta = max(deltas, key=lambda item: item[1])
    down_tick, down_delta = min(deltas, key=lambda item: item[1])
    if abs(up_delta) >= abs(down_delta):
        direction = "up"
        peak_tick = up_tick
        move_size = up_delta
    else:
        direction = "down"
        peak_tick = down_tick
        move_size = abs(down_delta)
    if move_size < threshold:
        return None
    end_tick = window_ticks[-1]
    end_ts = to_datetime(end_tick.get("observed_at"))
    peak_ts = to_datetime(peak_tick.get("observed_at"))
    if end_ts is None or peak_ts is None:
        return None
    signed_end_delta = _signed_move(_float_value(end_tick.get("mid")) - start_mid, direction)
    retrace = (move_size - signed_end_delta) / move_size if move_size else 0.0
    threshold_tick_index = _threshold_tick_index(window_ticks, start_mid, direction, threshold)
    event_type = _event_type(window_ticks, start_mid, direction, threshold_tick_index, retrace)
    resolution_label, _ = _tick_resolution(window_ticks)
    risk_tags = []
    if len(window_ticks) < PRICE_EVENT_MIN_TICKS:
        risk_tags.append("sparse_ticks")
    if resolution_label in {"minute_floor", "sparse"}:
        risk_tags.append(resolution_label)
    spread = _float_value(start_tick.get("spread"))
    if spread is not None and spread >= 0.05:
        risk_tags.append("wide_spread")
    liquidity = _float_value(start_tick.get("liquidity"))
    if liquidity is None:
        risk_tags.append("missing_liquidity")
    price_event_id = stable_hash([case_id, market_slug, start_ts.isoformat(), end_ts.isoformat(), window_label, direction, round(move_size, 4)])[:24]
    return {
        "price_event_id": price_event_id,
        "case_id": case_id,
        "market_slug": market_slug,
        "token_id": start_tick.get("token_id"),
        "start_at": start_ts.isoformat(),
        "peak_at": peak_ts.isoformat(),
        "end_at": end_ts.isoformat(),
        "direction": direction,
        "move_size": move_size,
        "duration_seconds": (end_ts - start_ts).total_seconds(),
        "pre_event_volatility": None,
        "spread_at_start": spread,
        "liquidity_at_start": liquidity,
        "price_data_resolution": resolution_label,
        "event_type": event_type,
        "detection_config_hash": config_hash,
        "risk_tags": risk_tags,
    }


def _event_type(
    window_ticks: Sequence[Mapping[str, object]],
    start_mid: float,
    direction: str,
    threshold_tick_index: Optional[int],
    retrace: float,
) -> str:
    if retrace > 0.5:
        return "reversal"
    if threshold_tick_index is not None and threshold_tick_index <= 2:
        return "jump"
    signed_steps = []
    previous_mid = start_mid
    for tick in window_ticks[1:]:
        mid = _float_value(tick.get("mid"))
        if mid is None:
            continue
        signed_steps.append(_signed_move(mid - previous_mid, direction))
        previous_mid = mid
    if signed_steps:
        non_negative = sum(1 for step in signed_steps if step >= -0.002) / len(signed_steps)
        if non_negative >= 0.65:
            return "ramp"
    return "chop"


def _dedupe_price_events(candidates: Sequence[dict]) -> list[dict]:
    ordered = sorted(candidates, key=lambda row: (str(row["start_at"]), -float(row.get("move_size") or 0)))
    result: list[dict] = []
    for event in ordered:
        event_start = to_datetime(event.get("start_at"))
        event_end = to_datetime(event.get("end_at"))
        if event_start is None or event_end is None:
            continue
        if not result:
            result.append(event)
            continue
        previous = result[-1]
        previous_end = to_datetime(previous.get("end_at"))
        if previous_end is not None and event_start <= previous_end:
            if float(event.get("move_size") or 0) > float(previous.get("move_size") or 0):
                result[-1] = event
        else:
            result.append(event)
    return result


def _samples_for_match(
    run_id: str,
    case_id: str,
    match: Mapping[str, object],
    event: Mapping[str, object],
    ticks: Sequence[Mapping[str, object]],
    horizons: Sequence[str],
) -> list[dict]:
    post_ts = to_datetime(match.get("post_created_at"))
    if post_ts is None:
        return []
    entry = _nearest_tick(ticks, post_ts, before=False)
    if not entry:
        return []
    entry_ts = to_datetime(entry.get("observed_at"))
    entry_price = _float_value(entry.get("mid"))
    if entry_ts is None or entry_price is None:
        return []
    direction = str(event.get("direction") or "up")
    rows = []
    for horizon in horizons:
        end_ts = entry_ts + timedelta(seconds=parse_duration(horizon))
        window = [tick for tick in ticks if _between(tick, entry_ts, end_ts)]
        if not window:
            continue
        future = window[-1]
        future_price = _float_value(future.get("mid"))
        if future_price is None:
            continue
        moves = [_signed_move(_float_value(tick.get("mid")) - entry_price, direction) for tick in window if _float_value(tick.get("mid")) is not None]
        if not moves:
            continue
        gross_delta = _signed_move(future_price - entry_price, direction)
        max_favorable = max(moves)
        max_adverse = min(moves)
        execution_cost = _round_trip_execution_cost(entry, future)
        net_delta = gross_delta - execution_cost
        net_max_favorable = max_favorable - execution_cost
        reward_to_risk = _reward_to_risk(net_max_favorable, max_adverse, execution_cost)
        risk_adjusted_edge = net_max_favorable + min(0.0, max_adverse)
        risk_tags = list(event.get("risk_tags") or [])
        relative = str(match.get("relative_position") or "")
        if relative != "before":
            risk_tags.append("not_pre_move_alpha")
        if max_favorable >= PAPER_MIN_EDGE and net_max_favorable < PAPER_MIN_EDGE:
            risk_tags.append("cost_erased_move")
        if reward_to_risk is not None and reward_to_risk < PAPER_MIN_REWARD_TO_RISK:
            risk_tags.append("poor_reward_to_risk")
        if max_adverse <= -PAPER_MAX_ADVERSE:
            risk_tags.append("adverse_excursion")
        paper_positive = (
            relative == "before"
            and net_max_favorable >= PAPER_MIN_EDGE
            and max_adverse > -PAPER_MAX_ADVERSE
            and reward_to_risk is not None
            and reward_to_risk >= PAPER_MIN_REWARD_TO_RISK
        )
        sample_id = stable_hash([match["price_event_id"], match["post_id"], horizon])[:24]
        rows.append(
            {
                "run_id": run_id,
                "sample_id": sample_id,
                "case_id": case_id,
                "price_event_id": match["price_event_id"],
                "post_id": match["post_id"],
                "handle": match["handle"],
                "market_slug": match["market_slug"],
                "horizon": horizon,
                "relative_position": relative,
                "entry_at": entry_ts.isoformat(),
                "entry_price": entry_price,
                "future_price": future_price,
                "gross_delta": gross_delta,
                "net_delta": net_delta,
                "max_favorable_delta": max_favorable,
                "max_adverse_delta": max_adverse,
                "execution_cost": execution_cost,
                "reward_to_risk": reward_to_risk,
                "risk_adjusted_edge": risk_adjusted_edge,
                "paper_trade_positive": paper_positive,
                "risk_tags": list(dict.fromkeys(risk_tags)),
            }
        )
    return rows


def _account_metrics(run_id: str, case_id: str, samples: Sequence[dict]) -> list[dict]:
    preferred = _preferred_samples(samples)
    by_handle: dict[str, list[dict]] = defaultdict(list)
    for sample in preferred:
        by_handle[str(sample["handle"])].append(sample)
    rows = []
    for handle, account_samples in by_handle.items():
        total = len(account_samples)
        before = sum(1 for sample in account_samples if sample.get("relative_position") == "before")
        during = sum(1 for sample in account_samples if sample.get("relative_position") == "during")
        late = sum(1 for sample in account_samples if sample.get("relative_position") == "after")
        tradable = sum(1 for sample in account_samples if sample.get("paper_trade_positive"))
        expectancy = _avg(sample.get("net_delta") for sample in account_samples) or 0.0
        before_rate = before / total if total else 0.0
        tradable_hit_rate = tradable / before if before else 0.0
        sample_confidence = min(1.0, total / FULL_CONFIDENCE_SAMPLES)
        if total < MIN_SOURCE_SAMPLES and tradable > 0:
            status = "needs_more_samples"
        elif before >= MIN_SOURCE_SAMPLES and tradable_hit_rate >= 0.5 and expectancy > 0:
            status = "price_first_source"
        elif total and during / total >= 0.5:
            status = "during_move_amplifier"
        elif total and late / total >= 0.5:
            status = "late_commentator"
        else:
            status = "watch"
        rows.append(
            {
                "run_id": run_id,
                "case_id": case_id,
                "handle": handle,
                "sample_size": total,
                "before_move_rate": before_rate,
                "during_move_rate": during / total if total else 0.0,
                "late_after_price_move_rate": late / total if total else 0.0,
                "tradable_hit_rate": tradable_hit_rate,
                "false_fomo_rate": 1 - tradable_hit_rate if before else None,
                "expectancy_after_cost": expectancy,
                "sample_confidence": sample_confidence,
                "recommended_status": status,
            }
        )
    return sorted(rows, key=lambda row: (row["tradable_hit_rate"], row["expectancy_after_cost"], row["sample_confidence"]), reverse=True)


def _preferred_samples(samples: Sequence[dict]) -> list[dict]:
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for sample in samples:
        by_key[(str(sample.get("price_event_id")), str(sample.get("post_id")))].append(sample)
    result = []
    for rows in by_key.values():
        for horizon in ("5m", "10m", "1m", "30m", "2h"):
            match = next((row for row in rows if row.get("horizon") == horizon), None)
            if match:
                result.append(match)
                break
        else:
            result.append(rows[0])
    return result


def _source_query_for_case(con: duckdb.DuckDBPyConnection, case: Mapping[str, object]) -> str:
    terms = _case_terms(con, case)
    query_terms = []
    for term in terms:
        if len(term) < 3:
            continue
        query_terms.append(f'"{term}"' if " " in term else term)
        if len(query_terms) >= 14:
            break
    return f"({' OR '.join(query_terms)}) -is:retweet" if query_terms else "-is:retweet"


def _case_terms(con: duckdb.DuckDBPyConnection, case: Mapping[str, object]) -> list[str]:
    terms = []
    terms.extend(str(term).lower() for term in (case.get("keywords") or []) if term)
    terms.extend(case_keywords(str(case.get("query") or "")))
    market_slug = case.get("market_slug")
    if market_slug:
        row = con.execute("select question, market_slug, tags from markets where market_slug = ? limit 1", [market_slug]).fetchone()
        if row:
            question, slug, tags = row
            terms.extend(words(str(question or "")))
            terms.extend(words(str(slug or "")))
            terms.extend(str(tag).lower() for tag in _json_list(tags))
    return list(dict.fromkeys(term for term in terms if term))


def _source_posts_for_case(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    rows = []
    for query in (
        """
        select post_id, handle, created_at, text, direction
        from event_case_posts
        where case_id = ?
        """,
        """
        select post_id, handle, created_at, text, null as direction
        from x_posts
        """,
        """
        select post_id, handle, created_at, text, null as direction
        from social_posts
        where platform = 'x'
        """,
    ):
        try:
            result = con.execute(query, [case_id] if "event_case_posts" in query else []).fetchall()
        except duckdb.CatalogException:
            continue
        columns = [desc[0] for desc in con.description]
        rows.extend(dict(zip(columns, row)) for row in result)
    deduped = {}
    for row in rows:
        if row.get("post_id"):
            deduped[str(row["post_id"])] = row
    return list(deduped.values())


def _semantic_confidence_by_post(con: duckdb.DuckDBPyConnection, case_id: str) -> dict[str, float]:
    rows = con.execute(
        """
        select post_id, max(coalesce(similarity, 0))
        from post_market_semantic_matches
        where case_id = ? and method = 'cloud'
        group by post_id
        """,
        [case_id],
    ).fetchall()
    return {str(post_id): float(similarity or 0.0) for post_id, similarity in rows}


def _market_ticks(con: duckdb.DuckDBPyConnection, market_slug: str, token_id: Optional[object]) -> list[dict]:
    rows = con.execute(
        """
        select observed_at, market_slug, token_id, best_bid, best_ask, mid, spread, liquidity, tick_source
        from market_ticks
        where market_slug = ? and mid is not null and (? is null or token_id = ?)
        order by observed_at
        """,
        [market_slug, token_id, token_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _yes_token_id(con: duckdb.DuckDBPyConnection, market_slug: str) -> Optional[str]:
    row = con.execute("select clob_token_ids from markets where market_slug = ? limit 1", [market_slug]).fetchone()
    if not row:
        return None
    tokens = _json_list(row[0])
    return tokens[0] if tokens else None


def _matched_terms(text: str, terms: Sequence[str]) -> list[str]:
    lowered = text.lower()
    return [term for term in terms if term and term.lower() in lowered]


def _keyword_confidence(matched_terms: Sequence[str]) -> float:
    if not matched_terms:
        return 0.0
    return min(0.95, 0.62 + 0.08 * len(set(matched_terms)))


def _relative_position(post_ts: datetime, start_at: datetime, end_at: datetime) -> str:
    if post_ts < start_at:
        return "before"
    if post_ts <= end_at:
        return "during"
    return "after"


def _direction_agrees(post_direction: object, event_direction: str) -> Optional[bool]:
    if post_direction == "watch_only" or not post_direction:
        return None
    return (post_direction == "bullish" and event_direction == "up") or (post_direction == "bearish" and event_direction == "down")


def _threshold_tick_index(ticks: Sequence[Mapping[str, object]], start_mid: float, direction: str, threshold: float) -> Optional[int]:
    for index, tick in enumerate(ticks):
        mid = _float_value(tick.get("mid"))
        if mid is not None and _signed_move(mid - start_mid, direction) >= threshold:
            return index
    return None


def _signed_move(raw_move: Optional[float], direction: str) -> float:
    if raw_move is None:
        return 0.0
    return -float(raw_move) if direction == "down" else float(raw_move)


def _between(tick: Mapping[str, object], start_at: datetime, end_at: datetime) -> bool:
    tick_ts = to_datetime(tick.get("observed_at"))
    return tick_ts is not None and start_at <= tick_ts <= end_at


def _nearest_tick(ticks: Sequence[Mapping[str, object]], timestamp: datetime, before: bool = False) -> Optional[Mapping[str, object]]:
    eligible = [tick for tick in ticks if (to_datetime(tick.get("observed_at")) or datetime.max.replace(tzinfo=timezone.utc)) <= timestamp] if before else [
        tick for tick in ticks if (to_datetime(tick.get("observed_at")) or datetime.max.replace(tzinfo=timezone.utc)) >= timestamp
    ]
    if not eligible:
        return None
    return eligible[-1] if before else eligible[0]


def _tick_resolution(ticks: Sequence[Mapping[str, object]]) -> tuple[str, Optional[float]]:
    stamps = [to_datetime(tick.get("observed_at")) for tick in ticks]
    stamps = [stamp for stamp in stamps if stamp is not None]
    if len(stamps) < 2:
        return "sparse", None
    gaps = sorted((stamps[index] - stamps[index - 1]).total_seconds() for index in range(1, len(stamps)))
    median_gap = gaps[len(gaps) // 2]
    sources = {str(tick.get("tick_source") or "") for tick in ticks}
    if median_gap <= 1.5:
        return "1s", median_gap
    if median_gap <= 10:
        return "10s", median_gap
    if median_gap <= 30:
        return "30s", median_gap
    if median_gap <= 75:
        return ("minute_floor" if any(source.startswith("historical") for source in sources) else "1m", median_gap)
    return "sparse", median_gap


def _round_trip_execution_cost(entry_tick: Mapping[str, object], exit_tick: Optional[Mapping[str, object]]) -> float:
    entry_half = _half_spread(entry_tick)
    exit_half = _half_spread(exit_tick) if exit_tick is not None else None
    if entry_half is None and exit_half is None:
        return PAPER_MIN_ROUND_TRIP_COST
    known_cost = sum(value for value in (entry_half, exit_half) if value is not None)
    missing_halves = int(entry_half is None) + int(exit_half is None)
    return max(PAPER_MIN_ROUND_TRIP_COST, known_cost + missing_halves * (PAPER_MIN_ROUND_TRIP_COST / 2) + PAPER_SLIPPAGE_BUFFER)


def _half_spread(tick: Optional[Mapping[str, object]]) -> Optional[float]:
    if not tick:
        return None
    spread = _float_value(tick.get("spread"))
    if spread is not None and spread > 0:
        return spread / 2
    bid = _float_value(tick.get("best_bid"))
    ask = _float_value(tick.get("best_ask"))
    if bid is not None and ask is not None and ask >= bid:
        return (ask - bid) / 2
    return None


def _reward_to_risk(net_max_favorable: float, max_adverse: float, execution_cost: float) -> Optional[float]:
    if net_max_favorable <= 0:
        return None
    denominator = max(abs(min(0.0, max_adverse)), execution_cost, PAPER_MIN_ROUND_TRIP_COST / 2)
    return net_max_favorable / denominator


def _count_total(counts: Mapping[str, object]) -> Optional[int]:
    meta = counts.get("meta")
    if isinstance(meta, Mapping) and meta.get("total_tweet_count") is not None:
        return int(meta["total_tweet_count"])
    data = counts.get("data")
    if isinstance(data, list):
        total = 0
        found = False
        for item in data:
            if isinstance(item, Mapping) and item.get("tweet_count") is not None:
                total += int(item["tweet_count"])
                found = True
        return total if found else None
    return None


def _samples_for_run(con: duckdb.DuckDBPyConnection, run_id: str) -> list[dict]:
    rows = con.execute(
        """
        select run_id, sample_id, case_id, price_event_id, post_id, handle, market_slug,
               horizon, relative_position, entry_at, entry_price, future_price,
               gross_delta, net_delta, max_favorable_delta, max_adverse_delta,
               execution_cost, reward_to_risk, risk_adjusted_edge, paper_trade_positive,
               risk_tags, created_at
        from backtest_samples
        where run_id = ?
        order by handle, horizon
        """,
        [run_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        item["risk_tags"] = _loads_json(item.get("risk_tags"), [])
        result.append(item)
    return result


def _metrics_for_run(con: duckdb.DuckDBPyConnection, run_id: str) -> list[dict]:
    rows = con.execute(
        """
        select run_id, case_id, handle, sample_size, before_move_rate, during_move_rate,
               late_after_price_move_rate, tradable_hit_rate, false_fomo_rate,
               expectancy_after_cost, sample_confidence, recommended_status, created_at
        from account_backtest_metrics
        where run_id = ?
        order by tradable_hit_rate desc, expectancy_after_cost desc, sample_confidence desc
        """,
        [run_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _avg(values: Iterable[object]) -> Optional[float]:
    clean = [_float_value(value) for value in values]
    clean = [value for value in clean if value is not None]
    return sum(clean) / len(clean) if clean else None


def _float_value(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_list(value: object) -> list[str]:
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


def _loads_json(value: object, default):
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _x_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fmt(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return str(value)
    try:
        return f"{float(value):.4g}"
    except (TypeError, ValueError):
        return str(value)
