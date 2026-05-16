from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence

import duckdb

from .clients import GammaMarketClient, market_record_from_gamma
from .config import SIGNAL_REPORT_ROOT, Web3AccountConfig, parse_duration
from .models import MarketRecord
from .storage import (
    insert_historical_price_ticks,
    live_burst_runs_for_case,
    upsert_event_account_metrics,
    upsert_event_cases,
    upsert_event_case_posts,
    upsert_event_post_impacts,
    upsert_markets,
)
from .utils import stable_hash, to_datetime, utc_now_iso, words


TRUMP_CHINA_BULLISH = (
    "visit",
    "trip",
    "beijing",
    "xi",
    "summit",
    "invitation",
    "meeting",
    "访华",
    "会晤",
    "北京",
    "峰会",
)
TRUMP_CHINA_BEARISH = (
    "deny",
    "no plan",
    "unlikely",
    "delay",
    "cancel",
    "postpone",
    "否认",
    "暂无计划",
    "推迟",
    "取消",
)
TRUMP_CHINA_CATALYSTS = (
    "iran",
    "tehran",
    "nuclear",
    "araghchi",
    "sanction",
    "sanctions",
    "tariff",
    "tariffs",
    "trade talks",
    "us-china",
    "u.s.-china",
    "rare earths",
    "taiwan",
    "white house",
    "potus",
    "truth social",
    "strike",
    "ceasefire",
    "hostage",
    "israel",
    "hormuz",
    "war",
    "伊朗",
    "关税",
    "中美",
    "白宫",
    "制裁",
    "台海",
)
TRUMP_CHINA_CATALYST_BULLISH = (
    "progress",
    "deal",
    "agreement",
    "broker",
    "stabilize",
    "stable",
    "resume",
    "constructive",
    "will not postpone",
    "not postpone",
    "still planned",
    "schedule",
    "scheduled",
    "confirmed",
    "进展",
    "协议",
    "稳定",
    "恢复",
    "仍计划",
)
TRUMP_CHINA_CATALYST_BEARISH = (
    "unresolved",
    "not resolved",
    "collapse",
    "collapsed",
    "breakdown",
    "strike",
    "attack",
    "escalation",
    "escalate",
    "sanction",
    "sanctions",
    "tariff",
    "tariffs",
    "delay",
    "postpone",
    "cancel",
    "war",
    "risk",
    "denied",
    "否认",
    "未解决",
    "升级",
    "打击",
    "制裁",
    "关税",
    "推迟",
    "取消",
)
TRUMP_CHINA_CATALYST_WATCH = (
    "uncertain",
    "uncertainty",
    "talks",
    "negotiations",
    "response",
    "proposal",
    "framework",
    "scheduling",
    "schedule",
    "不确定",
    "谈判",
)
DEFAULT_HORIZONS = ("1h", "6h", "24h", "72h")
DEFAULT_MICRO_HORIZONS = ("1s", "10s", "30s", "1m", "5m", "10m", "30m", "2h")
MICRO_CORE_HORIZONS = {"1m", "5m", "10m"}
PAPER_TRADE_MIN_ROUND_TRIP_COST = 0.01
PAPER_TRADE_SLIPPAGE_BUFFER = 0.002
PAPER_TRADE_MIN_EDGE = 0.03
PAPER_TRADE_STRONG_EDGE = 0.08
PAPER_TRADE_MAX_ADVERSE = 0.04
PAPER_TRADE_MIN_REWARD_TO_RISK = 1.5
PAPER_TRADE_MIN_RISK_ADJUSTED_EDGE = 0.015
MICRO_SOURCE_MIN_SAMPLES = 3
MICRO_SOURCE_FULL_CONFIDENCE_SAMPLES = 8


def slugify_case_id(query: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_")
    return slug[:64] or stable_hash(query)[:12]


def discover_event_case(
    con: duckdb.DuckDBPyConnection,
    query: str,
    start_at: str,
    end_at: str,
    case_id: Optional[str] = None,
    max_pages: int = 4,
    market_slug: Optional[str] = None,
) -> dict:
    case_id = case_id or slugify_case_id(query)
    keywords = case_keywords(query)
    records = _candidate_markets(query, keywords, max_pages=max_pages)
    if market_slug:
        selected = next((record for record in records if record.market_slug == market_slug), None)
        if selected is None:
            selected = MarketRecord(
                market_slug=market_slug,
                event_slug=market_slug,
                question=market_slug,
                category="unknown",
                tags=[],
                end_time=None,
                resolution_source=None,
                clob_token_ids=[],
                liquidity=None,
                raw={"manual_market_slug": market_slug},
            )
    else:
        selected = records[0] if records else None
    if selected:
        upsert_markets(con, [selected])
    row = {
        "case_id": case_id,
        "query": query,
        "market_slug": selected.market_slug if selected else None,
        "start_at": start_at,
        "end_at": end_at,
        "keywords": keywords,
        "status": "active" if selected else "market_unresolved",
    }
    upsert_event_cases(con, [row])
    return {**row, "candidate_count": len(records), "selected_market": selected}


def normalize_price_history(
    payload: Mapping[str, object],
    token_to_market: Mapping[str, Mapping[str, object]],
    ingested_at: Optional[str] = None,
    tick_source: str = "historical",
) -> List[dict]:
    history = payload.get("history") if isinstance(payload, Mapping) else {}
    if not isinstance(history, Mapping):
        return []
    ingested_at = ingested_at or utc_now_iso()
    rows = []
    for token_id, points in history.items():
        if not isinstance(points, list):
            continue
        market = token_to_market.get(str(token_id), {})
        for point in points:
            if not isinstance(point, Mapping):
                continue
            ts = _point_timestamp(point)
            price = _point_price(point)
            if ts is None or price is None:
                continue
            rows.append(
                {
                    "observed_at": datetime.fromtimestamp(ts, timezone.utc).replace(microsecond=0).isoformat(),
                    "market_slug": market.get("market_slug"),
                    "token_id": str(token_id),
                    "mid": price,
                    "liquidity": market.get("liquidity"),
                    "tick_source": tick_source,
                    "ingested_at": ingested_at,
                    "raw": dict(point),
                }
            )
    return rows


def store_event_case_posts(con: duckdb.DuckDBPyConnection, case_id: str, posts: Iterable[dict], keywords: Sequence[str]) -> int:
    rows = []
    for post in posts:
        text = str(post.get("text") or "")
        matched = matched_keywords(text, keywords)
        if not matched:
            continue
        rows.append(
            {
                "case_id": case_id,
                "post_id": post.get("post_id"),
                "handle": post.get("handle"),
                "created_at": post.get("created_at"),
                "text": text,
                "direction": direction_from_text(text),
                "matched_keywords": matched,
                "raw_json": post.get("raw_json") or post,
            }
        )
    return upsert_event_case_posts(con, rows)


def run_event_backtest(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    horizons: Sequence[str] = DEFAULT_HORIZONS,
    mode: str = "ramp",
) -> tuple[list[dict], list[dict]]:
    case = get_event_case(con, case_id)
    if not case or not case.get("market_slug"):
        return [], []
    posts = _case_posts(con, case_id)
    ticks = _case_ticks(con, str(case["market_slug"]))
    if mode in {"volatility", "micro"}:
        study_posts = _dedupe_volatility_posts(posts)
    else:
        study_posts = _dedupe_directional_posts(posts)
    impacts = _post_impacts(case_id, str(case["market_slug"]), study_posts, ticks, horizons, mode=mode)
    metrics = _account_metrics(case_id, study_posts, impacts, mode=mode)
    upsert_event_post_impacts(con, impacts)
    upsert_event_account_metrics(con, metrics)
    return impacts, metrics


def write_event_backtest_report(con: duckdb.DuckDBPyConnection, case_id: str, report_root: Path = SIGNAL_REPORT_ROOT) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    case = get_event_case(con, case_id)
    path = report_root / f"event_backtest_{case_id}.md"
    metrics = _event_metrics(con, case_id)
    impacts = _event_impacts(con, case_id)
    posts = _case_posts(con, case_id)
    live_runs = _live_burst_runs(con, case_id)
    ramp_rows = [row for row in impacts if row.get("mode") in {"ramp", "micro"} or row.get("tradable_ramp") is not None]
    micro_rows = [row for row in impacts if row.get("mode") == "micro"]
    has_minute_floor = any("minute_floor" in (row.get("risk_tags") or []) for row in impacts)
    lines = [
        f"# Event Backtest: {case_id}",
        "",
        f"- Query: {case.get('query') if case else 'unknown'}",
        f"- Market: `{case.get('market_slug') if case else 'unknown'}`",
        f"- Window: {case.get('start_at') if case else 'n/a'} to {case.get('end_at') if case else 'n/a'}",
        f"- Posts: {len(posts)}",
        f"- Impacts: {len(impacts)}",
        f"- Modes: {', '.join(sorted({str(row.get('mode') or 'unknown') for row in impacts})) if impacts else 'n/a'}",
        f"- Price resolution warning: {'minute_floor; sub-minute historical horizons are not validated' if has_minute_floor else 'n/a'}",
        "",
        "## Account Ranking",
        "",
        "| Account | Lead | Tradable | Micro Hit | Sub-10m | Median TTP | Ramp Hit | Strong | Avg Fav | Already Hot | Samples | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    if not metrics:
        lines.append("| n/a | 0 | 0 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | 0 | no_samples |")
    for row in metrics:
        lines.append(
            f"| @{row['account']} | {_fmt(row['lead_score'])} | {_fmt(row.get('tradable_score'))} | "
            f"{_fmt(row.get('micro_hit_rate'))} | {_fmt(row.get('sub_10m_hit_rate'))} | "
            f"{_fmt(row.get('median_time_to_price_in'))} | "
            f"{_fmt(row.get('ramp_hit_rate') if row.get('ramp_hit_rate') is not None else row.get('hit_rate'))} | "
            f"{_fmt(row.get('strong_ramp_rate'))} | {_fmt(row.get('avg_max_favorable_delta'))} | "
            f"{_fmt(row.get('already_hot_rate'))} | "
            f"{row['sample_size']} | {row['recommended_status']} |"
        )
    if micro_rows:
        lines.extend(
            [
                "",
                "## Micro Ladder",
                "",
                "| Account | Horizon | Entry Delay | Entry | Future | Move | Net Move | Cost | Edge | R/R | Risk Adj | Time To 3pp | Max 10m | Net Max Fav | Reversal 30m | Resolution | Tags |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
            ]
        )
        for row in micro_rows[:120]:
            lines.append(
                f"| @{row['handle']} | {row['horizon']} | {_fmt(row.get('entry_delay_seconds'))} | "
                f"{_fmt(row.get('entry_mid'))} | {_fmt(row.get('future_mid'))} | "
                f"{_fmt(row.get('close_delta') if row.get('close_delta') is not None else row.get('delta'))} | "
                f"{_fmt(row.get('net_close_delta'))} | {_fmt(row.get('execution_cost'))} | "
                f"{_fmt(row.get('edge_after_cost'))} | {_fmt(row.get('reward_to_risk'))} | "
                f"{_fmt(row.get('risk_adjusted_edge'))} | {_fmt(row.get('time_to_3pp_seconds'))} | "
                f"{_fmt(row.get('max_move_10m'))} | "
                f"{_fmt(row.get('net_max_favorable_delta'))} | {_fmt(row.get('reversal_30m'))} | "
                f"{row.get('price_data_resolution') or 'n/a'} | "
                f"{', '.join(row.get('risk_tags') or [])} |"
            )
    if live_runs:
        lines.extend(
            [
                "",
                "## Live Micro Evidence",
                "",
                "| Trigger | Account | Confidence | Status | Ticks | 1s | 10s | 30s | 1m | 5m | 10m | Net 10m | Cost | R/R | Time To 3pp | Max 10m | Reversal 30m |",
                "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        by_post = defaultdict(list)
        for row in micro_rows:
            by_post[str(row.get("post_id"))].append(row)
        for run in live_runs[:30]:
            post_rows = by_post.get(str(run.get("post_id")), [])
            by_horizon = {str(row.get("horizon")): row for row in post_rows}
            best = _best_live_micro_row(post_rows)
            lines.append(
                f"| {run.get('post_id')} | @{run.get('handle') or 'n/a'} | {_fmt(run.get('confidence'))} | "
                f"{run.get('status')} | {run.get('ticks_written')} | "
                f"{_fmt(_row_move(by_horizon.get('1s')))} | {_fmt(_row_move(by_horizon.get('10s')))} | "
                f"{_fmt(_row_move(by_horizon.get('30s')))} | {_fmt(_row_move(by_horizon.get('1m')))} | "
                f"{_fmt(_row_move(by_horizon.get('5m')))} | {_fmt(_row_move(by_horizon.get('10m')))} | "
                f"{_fmt(_row_net_move(by_horizon.get('10m')))} | {_fmt(best.get('execution_cost') if best else None)} | "
                f"{_fmt(best.get('reward_to_risk') if best else None)} | "
                f"{_fmt(best.get('time_to_3pp_seconds') if best else None)} | "
                f"{_fmt(best.get('max_move_10m') if best else None)} | "
                f"{_fmt(best.get('reversal_30m') if best else None)} |"
            )
    lines.extend(
        [
            "",
            "## Ramp Opportunity",
            "",
            "| Account | Horizon | Entry | Entry Delay | Max Fav | Net Fav | Max Adv | Close | Net Close | Cost | R/R | Risk Adj | Price-In Time | Ramp Mins | Tags | Tradable |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- |",
        ]
    )
    if not ramp_rows:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | no_matched_posts | False |")
    for row in ramp_rows[:80]:
        lines.append(
            f"| @{row['handle']} | {row['horizon']} | {_fmt(row['entry_mid'])} | "
            f"{_fmt(row.get('entry_delay_seconds'))} | {_fmt(row['max_favorable_delta'])} | "
            f"{_fmt(row.get('net_max_favorable_delta'))} | {_fmt(row['max_adverse_delta'])} | "
            f"{_fmt(row.get('close_delta') if row.get('close_delta') is not None else row.get('delta'))} | "
            f"{_fmt(row.get('net_close_delta'))} | {_fmt(row.get('execution_cost'))} | "
            f"{_fmt(row.get('reward_to_risk'))} | {_fmt(row.get('risk_adjusted_edge'))} | "
            f"{row.get('price_in_time') or 'n/a'} | {_fmt(row.get('ramp_duration_minutes'))} | "
            f"{', '.join(row.get('risk_tags') or [])} | {row.get('tradable_ramp')} |"
        )
    lines.extend(
        [
            "",
            "## Post Evidence",
            "",
            "| Time | Account | Direction | Match | Text |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for post in posts[:50]:
        text = " ".join(str(post.get("text") or "").split())[:160]
        matched = ", ".join((post.get("matched_keywords") or [])[:8])
        lines.append(f"| {post['created_at']} | @{post['handle']} | {post['direction']} | {matched} | {text} |")
    lines.extend(
        [
            "",
            "## Price Impact Samples",
            "",
            "| Account | Horizon | Entry | Future | Delta | Net Delta | Favorable | Net Favorable | Adverse | R/R | Risk Adj | Late To Price | Positive | Paper Positive |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in impacts[:80]:
        lines.append(
            f"| @{row['handle']} | {row['horizon']} | {_fmt(row['entry_mid'])} | {_fmt(row['future_mid'])} | "
            f"{_fmt(row['delta'])} | {_fmt(row.get('net_close_delta'))} | "
            f"{_fmt(row['max_favorable_delta'])} | {_fmt(row.get('net_max_favorable_delta'))} | "
            f"{_fmt(row['max_adverse_delta'])} | {_fmt(row.get('reward_to_risk'))} | "
            f"{_fmt(row.get('risk_adjusted_edge'))} | {row['price_move_started_before_post']} | "
            f"{row['is_positive']} | {row.get('paper_trade_positive')} |"
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def get_event_case(con: duckdb.DuckDBPyConnection, case_id: str) -> Optional[dict]:
    row = con.execute(
        """
        select case_id, query, market_slug, start_at, end_at, keywords, status
        from event_cases
        where case_id = ?
        """,
        [case_id],
    ).fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in con.description]
    item = dict(zip(columns, row))
    item["keywords"] = _loads(item.get("keywords"), [])
    return item


def case_keywords(query: str) -> list[str]:
    base = sorted(words(query))
    if {"trump", "china"}.issubset(set(base)) or "访华" in query:
        base.extend(TRUMP_CHINA_BULLISH)
        base.extend(TRUMP_CHINA_BEARISH)
        base.extend(TRUMP_CHINA_CATALYSTS)
        base.extend(TRUMP_CHINA_CATALYST_BULLISH)
        base.extend(TRUMP_CHINA_CATALYST_BEARISH)
        base.extend(TRUMP_CHINA_CATALYST_WATCH)
        base.extend(["china", "trump", "中国"])
    return list(dict.fromkeys(term.lower() for term in base if term))


def matched_keywords(text: str, keywords: Sequence[str]) -> list[str]:
    lowered = text.lower()
    matches = [keyword for keyword in keywords if _keyword_in_text(keyword, lowered)]
    if _looks_like_trump_china_case(keywords):
        direct_match = _trump_china_direct_match(lowered)
        catalyst_match = _trump_china_catalyst_match(lowered)
        if not (direct_match or catalyst_match):
            return []
        if catalyst_match and "indirect_catalyst" not in matches:
            matches.append("indirect_catalyst")
    return matches


def direction_from_text(text: str) -> str:
    lowered = text.lower()
    bull = sum(1 for term in TRUMP_CHINA_BULLISH if _keyword_in_text(term, lowered))
    bull += sum(1 for term in TRUMP_CHINA_CATALYST_BULLISH if _keyword_in_text(term, lowered))
    bear = sum(1 for term in TRUMP_CHINA_BEARISH if _keyword_in_text(term, lowered))
    bear += sum(1 for term in TRUMP_CHINA_CATALYST_BEARISH if _keyword_in_text(term, lowered))
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "watch_only"


def x_case_query(handle: str, keywords: Sequence[str]) -> str:
    if _looks_like_trump_china_case(keywords):
        anchor_expr = '(trump OR "donald trump" OR potus OR "white house")'
        catalyst_expr = (
            '(china OR beijing OR xi OR "us-china" OR "china trip" OR iran OR tehran OR nuclear '
            'OR sanction OR sanctions OR tariff OR tariffs OR taiwan OR "rare earths" '
            'OR "trade talks" OR postpone OR delay OR cancel OR summit OR visit OR hormuz)'
        )
        return f"from:{handle.lstrip('@')} {anchor_expr} {catalyst_expr} -is:retweet"
    core = [keyword for keyword in keywords if keyword and len(keyword) > 1][:16]
    keyword_expr = " OR ".join(f'"{keyword}"' if " " in keyword else keyword for keyword in core)
    return f"from:{handle.lstrip('@')} ({keyword_expr}) -is:retweet"


def x_time(value: object) -> str:
    parsed = to_datetime(value)
    if parsed is None:
        parsed = datetime.now(timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def event_case_token_rows(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    case = get_event_case(con, case_id)
    if not case or not case.get("market_slug"):
        return []
    rows = con.execute(
        """
        select market_slug, clob_token_ids, liquidity
        from markets
        where market_slug = ?
        """,
        [case["market_slug"]],
    ).fetchall()
    result = []
    for market_slug, token_json, liquidity in rows:
        for token_id in _json_list(token_json):
            result.append({"market_slug": market_slug, "token_id": token_id, "liquidity": liquidity})
    return result


def event_price_windows(
    con: duckdb.DuckDBPyConnection,
    case_id: str,
    pre: str = "10m",
    post: str = "2h",
    max_windows: int = 200,
) -> list[tuple[datetime, datetime]]:
    pre_seconds = parse_duration(pre)
    post_seconds = parse_duration(post)
    windows = []
    for row in _case_posts(con, case_id):
        created_at = to_datetime(row.get("created_at"))
        if created_at is None:
            continue
        windows.append((created_at - timedelta(seconds=pre_seconds), created_at + timedelta(seconds=post_seconds)))
    return _merge_time_windows(windows)[:max_windows]


def _candidate_markets(query: str, keywords: Sequence[str], max_pages: int) -> List[MarketRecord]:
    client = GammaMarketClient()
    candidates = []
    for closed in (False, True):
        for item in client.list_markets(max_pages=max_pages, closed=closed):
            record = market_record_from_gamma(item)
            score = _market_score(record, query, keywords)
            if score > 0:
                candidates.append((score, record))
    candidates.sort(key=lambda item: item[0], reverse=True)
    seen = set()
    result = []
    for _, record in candidates:
        if record.market_slug in seen:
            continue
        seen.add(record.market_slug)
        result.append(record)
    return result


def _market_score(record: MarketRecord, query: str, keywords: Sequence[str]) -> int:
    haystack = " ".join([record.market_slug, record.question, record.event_slug or "", record.category or "", " ".join(record.tags)]).lower()
    score = 0
    for term in words(query):
        if term in haystack:
            score += 4
    for keyword in keywords:
        if keyword.lower() in haystack:
            score += 1
    return score


def _post_impacts(
    case_id: str,
    market_slug: str,
    posts: Sequence[dict],
    ticks: Sequence[dict],
    horizons: Sequence[str],
    mode: str = "ramp",
) -> list[dict]:
    if mode == "micro":
        return _micro_post_impacts(case_id, market_slug, posts, ticks, horizons)
    if mode == "ramp":
        return _ramp_post_impacts(case_id, market_slug, posts, ticks, horizons)
    if mode == "volatility":
        return _volatility_post_impacts(case_id, market_slug, posts, ticks, horizons)
    return _event_post_impacts(case_id, market_slug, posts, ticks, horizons)


def _micro_post_impacts(case_id: str, market_slug: str, posts: Sequence[dict], ticks: Sequence[dict], horizons: Sequence[str]) -> list[dict]:
    impacts = []
    for post in posts:
        post_ts = to_datetime(post["created_at"])
        if post_ts is None:
            continue
        entry = _nearest_tick(ticks, post_ts, before=False)
        if entry is None:
            continue
        entry_ts = to_datetime(entry["observed_at"])
        if entry_ts is None:
            continue
        entry_mid = float(entry["mid"])
        sign = 1 if post.get("direction") == "bullish" else -1 if post.get("direction") == "bearish" else 0
        entry_delay = max(0.0, (entry_ts - post_ts).total_seconds())
        pre_abs_move = _pre_abs_move_before_post(ticks, post_ts)
        late_after_price_move = pre_abs_move >= 0.03
        resolution_label, resolution_seconds = _tick_resolution(ticks, post_ts - timedelta(minutes=10), post_ts + timedelta(hours=2))
        crowded = entry_mid >= 0.90 or entry_mid <= 0.10
        for horizon in horizons:
            horizon_seconds = parse_duration(horizon)
            end_ts = post_ts + timedelta(seconds=horizon_seconds)
            risk_tags = ["micro_price_in"]
            if sign == 0:
                risk_tags.append("direction_unknown")
            if late_after_price_move:
                risk_tags.append("late_after_price_move")
            if crowded:
                risk_tags.append("crowded_entry")
            if resolution_label == "minute_floor":
                risk_tags.append("minute_floor")
            if resolution_seconds is not None and horizon_seconds < resolution_seconds:
                risk_tags.append("insufficient_resolution")
            if entry_delay > min(60.0, horizon_seconds):
                risk_tags.append("slow_entry_tick")

            window = [tick for tick in ticks if _between_tick(tick, entry_ts, end_ts)]
            ten_min_window = [tick for tick in ticks if _between_tick(tick, entry_ts, post_ts + timedelta(minutes=10))]
            thirty_min_window = [tick for tick in ticks if _between_tick(tick, entry_ts, post_ts + timedelta(minutes=30))]
            if not window:
                impacts.append(
                    _micro_impact_row(
                        case_id,
                        market_slug,
                        post,
                        horizon,
                        entry_mid,
                        None,
                        None,
                        None,
                        None,
                        entry_delay,
                        None,
                        None,
                        None,
                        late_after_price_move,
                        False,
                        False,
                        crowded,
                        resolution_label,
                        risk_tags + ["no_window_ticks"],
                        execution_cost=None,
                        net_close_delta=None,
                        net_max_favorable=None,
                        net_max_adverse=None,
                        edge_after_cost=None,
                        reward_to_risk=None,
                        risk_adjusted_edge=None,
                        paper_trade_positive=False,
                        paper_trade_strong=False,
                    )
                )
                continue
            future_mid = float(window[-1]["mid"])
            move_points = [
                (tick, _signed_or_abs_move(float(tick["mid"]) - entry_mid, sign))
                for tick in window
                if tick.get("mid") is not None
            ]
            raw_moves_10m = [_signed_or_abs_move(float(tick["mid"]) - entry_mid, sign) for tick in ten_min_window if tick.get("mid") is not None]
            raw_moves_30m = [_signed_or_abs_move(float(tick["mid"]) - entry_mid, sign) for tick in thirty_min_window if tick.get("mid") is not None]
            if not move_points:
                continue
            moves = [move for _, move in move_points]
            net_move_points = [(tick, move - _round_trip_execution_cost(entry, tick)) for tick, move in move_points]
            net_moves = [move for _, move in net_move_points]
            max_favorable = max(moves)
            max_adverse = min(moves)
            net_max_favorable = max(net_moves)
            net_max_adverse = min(net_moves)
            max_10m = max(raw_moves_10m) if raw_moves_10m else None
            close_30m = raw_moves_30m[-1] if raw_moves_30m else None
            reversal_30m = (max_10m - close_30m) if max_10m is not None and close_30m is not None else None
            close_delta = _signed_or_abs_move(future_mid - entry_mid, sign)
            execution_cost = _round_trip_execution_cost(entry, window[-1])
            net_close_delta = close_delta - execution_cost
            edge_after_cost = net_max_favorable
            reward_to_risk = _reward_to_risk(net_max_favorable, net_max_adverse, execution_cost)
            risk_adjusted_edge = _risk_adjusted_edge(net_max_favorable, net_max_adverse)
            price_tick = _first_threshold_tick(window, entry_mid, sign, threshold=0.03)
            time_to_3pp = None
            price_in_time = None
            if price_tick:
                price_in_ts = to_datetime(price_tick["observed_at"])
                if price_in_ts is not None:
                    time_to_3pp = max(0.0, (price_in_ts - entry_ts).total_seconds())
                    price_in_time = price_in_ts.isoformat()
            insufficient = "insufficient_resolution" in risk_tags
            micro_hit = (not insufficient) and (max_favorable >= 0.03)
            strong_micro = (not insufficient) and (max_favorable >= 0.08 and max_adverse > -0.05)
            paper_trade_positive = (not insufficient) and (net_max_favorable >= PAPER_TRADE_MIN_EDGE)
            paper_trade_strong = (
                (not insufficient)
                and (net_max_favorable >= PAPER_TRADE_STRONG_EDGE)
                and (net_max_adverse > -PAPER_TRADE_MAX_ADVERSE)
                and reward_to_risk is not None
                and reward_to_risk >= 2.0
            )
            if micro_hit and not paper_trade_positive:
                risk_tags.append("cost_erased_move")
            if execution_cost >= 0.02:
                risk_tags.append("high_execution_cost")
            if paper_trade_positive and reward_to_risk is not None and reward_to_risk < PAPER_TRADE_MIN_REWARD_TO_RISK:
                risk_tags.append("poor_reward_to_risk")
            if paper_trade_positive and net_max_adverse <= -PAPER_TRADE_MAX_ADVERSE:
                risk_tags.append("adverse_excursion")
            if paper_trade_positive and risk_adjusted_edge < PAPER_TRADE_MIN_RISK_ADJUSTED_EDGE:
                risk_tags.append("thin_risk_adjusted_edge")
            tradable = (
                micro_hit
                and paper_trade_positive
                and reward_to_risk is not None
                and reward_to_risk >= PAPER_TRADE_MIN_REWARD_TO_RISK
                and net_max_adverse > -PAPER_TRADE_MAX_ADVERSE
                and risk_adjusted_edge >= PAPER_TRADE_MIN_RISK_ADJUSTED_EDGE
                and entry_delay <= 60
                and not late_after_price_move
            )
            impacts.append(
                _micro_impact_row(
                    case_id,
                    market_slug,
                    post,
                    horizon,
                    entry_mid,
                    future_mid,
                    close_delta,
                    max_favorable,
                    max_adverse,
                    entry_delay,
                    price_in_time,
                    time_to_3pp,
                    max_10m,
                    late_after_price_move,
                    micro_hit,
                    strong_micro,
                    crowded,
                    resolution_label,
                    risk_tags,
                    tradable=tradable,
                    reversal_30m=reversal_30m,
                    execution_cost=execution_cost,
                    net_close_delta=net_close_delta,
                    net_max_favorable=net_max_favorable,
                    net_max_adverse=net_max_adverse,
                    edge_after_cost=edge_after_cost,
                    reward_to_risk=reward_to_risk,
                    risk_adjusted_edge=risk_adjusted_edge,
                    paper_trade_positive=paper_trade_positive,
                    paper_trade_strong=paper_trade_strong,
                )
            )
    return impacts


def _event_post_impacts(case_id: str, market_slug: str, posts: Sequence[dict], ticks: Sequence[dict], horizons: Sequence[str]) -> list[dict]:
    impacts = []
    for post in posts:
        post_ts = to_datetime(post["created_at"])
        if post_ts is None:
            continue
        entry = _nearest_tick(ticks, post_ts, before=True)
        if entry is None:
            continue
        entry_mid = float(entry["mid"])
        sign = 1 if post["direction"] == "bullish" else -1 if post["direction"] == "bearish" else 0
        if sign == 0:
            continue
        pre_move = _pre_move(ticks, post_ts, entry_mid, sign)
        late_to_price = abs(pre_move) >= 0.08
        for horizon in horizons:
            end_ts = post_ts + timedelta(seconds=parse_duration(horizon))
            window = [tick for tick in ticks if _between_tick(tick, post_ts, end_ts)]
            if not window:
                continue
            future = _nearest_tick(ticks, end_ts, before=False) or window[-1]
            future_mid = float(future["mid"])
            signed_moves = [(float(tick["mid"]) - entry_mid) * sign for tick in window]
            delta = (future_mid - entry_mid) * sign
            impacts.append(
                {
                    "mode": "event",
                    "case_id": case_id,
                    "post_id": post["post_id"],
                    "handle": post["handle"],
                    "market_slug": market_slug,
                    "horizon": horizon,
                    "entry_mid": entry_mid,
                    "future_mid": future_mid,
                    "delta": delta,
                    "close_delta": delta,
                    "max_favorable_delta": max(signed_moves),
                    "max_adverse_delta": min(signed_moves),
                    "entry_delay_seconds": None,
                    "price_in_time": None,
                    "ramp_duration_minutes": None,
                    "price_move_started_before_post": late_to_price,
                    "is_positive": (not late_to_price) and (delta >= 0.03 or max(signed_moves) >= 0.03),
                    "is_strong": (not late_to_price) and max(signed_moves) >= 0.08 and min(signed_moves) > -0.05,
                    "tradable_ramp": False,
                    "strong_ramp": False,
                    "already_hot_penalty": late_to_price,
                    "crowded_entry": False,
                    "late_stage_ramp": False,
                    "risk_tags": ["already_hot_penalty"] if late_to_price else [],
                }
            )
    return impacts


def _ramp_post_impacts(case_id: str, market_slug: str, posts: Sequence[dict], ticks: Sequence[dict], horizons: Sequence[str]) -> list[dict]:
    impacts = []
    for post in posts:
        post_ts = to_datetime(post["created_at"])
        if post_ts is None:
            continue
        entry = _nearest_tick(ticks, post_ts, before=False)
        if entry is None:
            continue
        entry_ts = to_datetime(entry["observed_at"])
        if entry_ts is None:
            continue
        entry_mid = float(entry["mid"])
        sign = 1 if post["direction"] == "bullish" else -1 if post["direction"] == "bearish" else 0
        if sign == 0:
            continue
        entry_delay = max(0.0, (entry_ts - post_ts).total_seconds())
        pre_move = _pre_move_before_post(ticks, post_ts, sign)
        already_hot = pre_move >= 0.08
        crowded = (sign > 0 and entry_mid >= 0.90) or (sign < 0 and entry_mid <= 0.10)
        for horizon in horizons:
            end_ts = post_ts + timedelta(seconds=parse_duration(horizon))
            window = [tick for tick in ticks if _between_tick(tick, entry_ts, end_ts)]
            if not window:
                continue
            future = window[-1]
            future_mid = float(future["mid"])
            signed_points = [
                (tick, (float(tick["mid"]) - entry_mid) * sign)
                for tick in window
                if tick.get("mid") is not None
            ]
            if not signed_points:
                continue
            price_in_tick, max_favorable = max(signed_points, key=lambda item: item[1])
            max_adverse = min(move for _, move in signed_points)
            close_delta = (future_mid - entry_mid) * sign
            price_in_ts = to_datetime(price_in_tick["observed_at"])
            ramp_minutes = (price_in_ts - entry_ts).total_seconds() / 60 if price_in_ts else None
            ramp_hit = max_favorable >= 0.03
            tradable_ramp = ramp_hit and entry_delay <= 15 * 60
            strong_ramp = max_favorable >= 0.08 and max_adverse > -0.05
            late_stage_ramp = crowded and ramp_hit
            risk_tags = []
            if already_hot:
                risk_tags.append("already_hot_penalty")
            if crowded:
                risk_tags.append("crowded_entry")
            if late_stage_ramp:
                risk_tags.append("late_stage_ramp")
            if entry_delay > 15 * 60:
                risk_tags.append("slow_entry_tick")
            impacts.append(
                {
                    "mode": "ramp",
                    "case_id": case_id,
                    "post_id": post["post_id"],
                    "handle": post["handle"],
                    "market_slug": market_slug,
                    "horizon": horizon,
                    "entry_mid": entry_mid,
                    "future_mid": future_mid,
                    "delta": close_delta,
                    "close_delta": close_delta,
                    "max_favorable_delta": max_favorable,
                    "max_adverse_delta": max_adverse,
                    "entry_delay_seconds": entry_delay,
                    "price_in_time": price_in_ts.isoformat() if price_in_ts else None,
                    "ramp_duration_minutes": ramp_minutes,
                    "price_move_started_before_post": already_hot,
                    "is_positive": ramp_hit,
                    "is_strong": strong_ramp,
                    "tradable_ramp": tradable_ramp,
                    "strong_ramp": strong_ramp,
                    "already_hot_penalty": already_hot,
                    "crowded_entry": crowded,
                    "late_stage_ramp": late_stage_ramp,
                    "risk_tags": risk_tags,
                }
            )
    return impacts


def _volatility_post_impacts(case_id: str, market_slug: str, posts: Sequence[dict], ticks: Sequence[dict], horizons: Sequence[str]) -> list[dict]:
    impacts = []
    for post in posts:
        post_ts = to_datetime(post["created_at"])
        if post_ts is None:
            continue
        entry = _nearest_tick(ticks, post_ts, before=False)
        if entry is None:
            continue
        entry_ts = to_datetime(entry["observed_at"])
        if entry_ts is None:
            continue
        entry_mid = float(entry["mid"])
        entry_delay = max(0.0, (entry_ts - post_ts).total_seconds())
        pre_abs_move = _pre_abs_move_before_post(ticks, post_ts)
        already_volatile = pre_abs_move >= 0.08
        crowded = entry_mid >= 0.90 or entry_mid <= 0.10
        for horizon in horizons:
            end_ts = post_ts + timedelta(seconds=parse_duration(horizon))
            window = [tick for tick in ticks if _between_tick(tick, entry_ts, end_ts)]
            if not window:
                continue
            future = window[-1]
            future_mid = float(future["mid"])
            move_points = [
                (tick, float(tick["mid"]) - entry_mid)
                for tick in window
                if tick.get("mid") is not None
            ]
            if not move_points:
                continue
            price_in_tick, price_in_move = max(move_points, key=lambda item: abs(item[1]))
            max_abs_move = abs(price_in_move)
            max_up_move = max(move for _, move in move_points)
            max_down_move = min(move for _, move in move_points)
            close_delta = future_mid - entry_mid
            price_in_ts = to_datetime(price_in_tick["observed_at"])
            ramp_minutes = (price_in_ts - entry_ts).total_seconds() / 60 if price_in_ts else None
            volatility_hit = max_abs_move >= 0.03
            strong_volatility = max_abs_move >= 0.08
            tradable = volatility_hit and entry_delay <= 15 * 60
            risk_tags = ["two_sided_volatility"]
            if post.get("direction") == "watch_only":
                risk_tags.append("direction_unknown")
            if already_volatile:
                risk_tags.append("already_volatile")
            if crowded:
                risk_tags.append("crowded_entry")
            if entry_delay > 15 * 60:
                risk_tags.append("slow_entry_tick")
            impacts.append(
                {
                    "mode": "volatility",
                    "case_id": case_id,
                    "post_id": post["post_id"],
                    "handle": post["handle"],
                    "market_slug": market_slug,
                    "horizon": horizon,
                    "entry_mid": entry_mid,
                    "future_mid": future_mid,
                    "delta": close_delta,
                    "close_delta": close_delta,
                    "max_favorable_delta": max_abs_move,
                    "max_adverse_delta": max_down_move,
                    "entry_delay_seconds": entry_delay,
                    "price_in_time": price_in_ts.isoformat() if price_in_ts else None,
                    "ramp_duration_minutes": ramp_minutes,
                    "price_move_started_before_post": already_volatile,
                    "is_positive": volatility_hit,
                    "is_strong": strong_volatility,
                    "tradable_ramp": tradable,
                    "strong_ramp": strong_volatility,
                    "already_hot_penalty": already_volatile,
                    "crowded_entry": crowded,
                    "late_stage_ramp": crowded and volatility_hit,
                    "risk_tags": risk_tags,
                }
            )
    return impacts


def _account_metrics(case_id: str, posts: Sequence[dict], impacts: Sequence[dict], mode: str = "ramp") -> list[dict]:
    if mode == "micro":
        return _micro_account_metrics(case_id, posts, impacts)
    if mode == "volatility":
        return _volatility_account_metrics(case_id, posts, impacts)
    if mode == "ramp":
        return _ramp_account_metrics(case_id, posts, impacts)
    return _event_account_metrics(case_id, posts, impacts)


def _micro_account_metrics(case_id: str, posts: Sequence[dict], impacts: Sequence[dict]) -> list[dict]:
    by_account = defaultdict(list)
    preferred = _preferred_micro_impacts(impacts)
    for impact in preferred:
        by_account[impact["handle"]].append(impact)
    first_posts = sorted(posts, key=lambda item: item["created_at"])
    lead_rank = {post["handle"]: idx for idx, post in enumerate(first_posts)}
    rows = []
    for account, samples in by_account.items():
        usable = [row for row in samples if "insufficient_resolution" not in (row.get("risk_tags") or [])]
        hits = sum(1 for row in usable if row.get("is_positive"))
        paper_hits = sum(1 for row in usable if row.get("paper_trade_positive"))
        strong = sum(1 for row in usable if row.get("is_strong") or row.get("strong_ramp"))
        tradable = sum(1 for row in usable if row.get("tradable_ramp"))
        late = sum(1 for row in usable if row.get("already_hot_penalty") or row.get("price_move_started_before_post"))
        sub_10m = sum(
            1
            for row in usable
            if row.get("time_to_3pp_seconds") is not None and float(row["time_to_3pp_seconds"]) <= 10 * 60
        )
        hit_rate = hits / len(usable) if usable else 0
        paper_hit_rate = paper_hits / len(usable) if usable else 0
        strong_rate = strong / len(usable) if usable else 0
        late_rate = late / len(usable) if usable else 0
        sub_10m_rate = sub_10m / len(usable) if usable else 0
        false_fomo = 1 - paper_hit_rate if usable else None
        sample_confidence = min(1.0, len(usable) / MICRO_SOURCE_FULL_CONFIDENCE_SAMPLES) if usable else 0.0
        shrunk_paper_hit_rate = (paper_hits + 1) / (len(usable) + 2) if usable else 0.0
        shrunk_sub_10m_rate = (sub_10m + 1) / (len(usable) + 2) if usable else 0.0
        lead_score = max(0, 25 - lead_rank.get(account, 5) * 5)
        avg_fav = _avg(row.get("max_favorable_delta") for row in usable) or 0
        avg_adv = _avg(row.get("max_adverse_delta") for row in usable) or 0
        avg_reward_to_risk = _avg(row.get("reward_to_risk") for row in usable) or 0
        avg_risk_adjusted_edge = _avg(row.get("risk_adjusted_edge") for row in usable) or 0
        median_ttp = _median(row.get("time_to_3pp_seconds") for row in usable)
        raw_tradable_score = max(
            0.0,
            min(
                100.0,
                shrunk_paper_hit_rate * 30
                + shrunk_sub_10m_rate * 20
                + strong_rate * 15
                + min(15.0, avg_fav * 250)
                + min(10.0, max(0.0, avg_risk_adjusted_edge) * 350)
                + min(7.5, max(0.0, avg_reward_to_risk) * 1.5)
                + lead_score * 0.25
                - late_rate * 20
                - max(0.0, -avg_adv) * 80,
            ),
        )
        tradable_score = raw_tradable_score * (0.35 + 0.65 * sample_confidence)
        if samples and not usable:
            status = "insufficient_resolution"
        elif len(usable) < MICRO_SOURCE_MIN_SAMPLES and paper_hits > 0:
            status = "needs_more_samples"
        elif tradable > 0 and paper_hits > 0 and sub_10m > 0:
            status = "micro_source"
        elif paper_hits > 0:
            status = "watch"
        elif hits > 0:
            status = "cost_erased_watch"
        else:
            status = "noise_or_no_micro_ramp"
        rows.append(
            {
                "account": account,
                "case_id": case_id,
                "lead_score": lead_score,
                "impact_score": tradable_score,
                "hit_rate": hit_rate,
                "false_fomo_rate": false_fomo,
                "sample_size": len(samples),
                "ramp_hit_rate": hit_rate,
                "strong_ramp_rate": strong_rate,
                "avg_entry_delay_seconds": _avg(row.get("entry_delay_seconds") for row in usable),
                "avg_max_favorable_delta": avg_fav,
                "avg_max_adverse_delta": avg_adv,
                "already_hot_rate": late_rate if usable else None,
                "tradable_score": tradable_score,
                "micro_hit_rate": hit_rate,
                "median_time_to_price_in": median_ttp,
                "sub_10m_hit_rate": sub_10m_rate,
                "late_after_price_move_rate": late_rate if usable else None,
                "recommended_status": status,
            }
        )
    return sorted(rows, key=lambda item: (item["tradable_score"], item["lead_score"]), reverse=True)


def _event_account_metrics(case_id: str, posts: Sequence[dict], impacts: Sequence[dict]) -> list[dict]:
    by_account = defaultdict(list)
    preferred = _preferred_horizon_impacts(impacts)
    for impact in preferred:
        by_account[impact["handle"]].append(impact)
    first_posts = sorted(posts, key=lambda item: item["created_at"])
    lead_rank = {post["handle"]: idx for idx, post in enumerate(first_posts)}
    rows = []
    for account, samples in by_account.items():
        positives = sum(1 for row in samples if row["is_positive"])
        late = sum(1 for row in samples if row["price_move_started_before_post"])
        strong = sum(1 for row in samples if row["is_strong"])
        hit_rate = positives / len(samples) if samples else 0
        false_fomo = 1 - hit_rate if samples else None
        lead_score = max(0, 25 - lead_rank.get(account, 5) * 5)
        impact_score = min(50, hit_rate * 30 + strong * 8 - late * 10)
        status = "ranked" if len(samples) >= 3 and hit_rate >= 0.5 and late == 0 else "watch" if positives else "late_or_noise"
        rows.append(
            {
                "account": account,
                "case_id": case_id,
                "lead_score": lead_score,
                "impact_score": max(0, impact_score),
                "hit_rate": hit_rate,
                "false_fomo_rate": false_fomo,
                "sample_size": len(samples),
                "ramp_hit_rate": hit_rate,
                "strong_ramp_rate": strong / len(samples) if samples else None,
                "avg_entry_delay_seconds": None,
                "avg_max_favorable_delta": _avg(row.get("max_favorable_delta") for row in samples),
                "avg_max_adverse_delta": _avg(row.get("max_adverse_delta") for row in samples),
                "already_hot_rate": late / len(samples) if samples else None,
                "tradable_score": max(0, impact_score),
                "recommended_status": status,
            }
        )
    return sorted(rows, key=lambda item: (item["impact_score"], item["lead_score"]), reverse=True)


def _ramp_account_metrics(case_id: str, posts: Sequence[dict], impacts: Sequence[dict]) -> list[dict]:
    by_account = defaultdict(list)
    preferred = _preferred_horizon_impacts(impacts)
    for impact in preferred:
        by_account[impact["handle"]].append(impact)
    first_posts = sorted(posts, key=lambda item: item["created_at"])
    lead_rank = {post["handle"]: idx for idx, post in enumerate(first_posts)}
    rows = []
    for account, samples in by_account.items():
        ramp_hits = sum(1 for row in samples if row.get("is_positive"))
        tradable = sum(1 for row in samples if row.get("tradable_ramp"))
        strong = sum(1 for row in samples if row.get("strong_ramp") or row.get("is_strong"))
        already_hot = sum(1 for row in samples if row.get("already_hot_penalty"))
        hit_rate = ramp_hits / len(samples) if samples else 0
        strong_rate = strong / len(samples) if samples else 0
        already_hot_rate = already_hot / len(samples) if samples else 0
        false_fomo = 1 - hit_rate if samples else None
        lead_score = max(0, 25 - lead_rank.get(account, 5) * 5)
        avg_fav = _avg(row.get("max_favorable_delta") for row in samples) or 0
        avg_adv = _avg(row.get("max_adverse_delta") for row in samples) or 0
        avg_delay = _avg(row.get("entry_delay_seconds") for row in samples)
        tradable_score = max(
            0.0,
            min(
                100.0,
                hit_rate * 35
                + strong_rate * 25
                + min(20.0, avg_fav * 200)
                + lead_score * 0.4
                - max(0.0, -avg_adv) * 80
                - already_hot_rate * 10,
            ),
        )
        if tradable > 0 and strong > 0:
            status = "ramp_source"
        elif tradable > 0:
            status = "watch"
        elif ramp_hits > 0 and already_hot_rate >= 0.5:
            status = "late_stage_only"
        else:
            status = "noise_or_no_ramp"
        rows.append(
            {
                "account": account,
                "case_id": case_id,
                "lead_score": lead_score,
                "impact_score": tradable_score,
                "hit_rate": hit_rate,
                "false_fomo_rate": false_fomo,
                "sample_size": len(samples),
                "ramp_hit_rate": hit_rate,
                "strong_ramp_rate": strong_rate,
                "avg_entry_delay_seconds": avg_delay,
                "avg_max_favorable_delta": avg_fav,
                "avg_max_adverse_delta": avg_adv,
                "already_hot_rate": already_hot_rate,
                "tradable_score": tradable_score,
                "recommended_status": status,
            }
        )
    return sorted(rows, key=lambda item: (item["tradable_score"], item["lead_score"]), reverse=True)


def _volatility_account_metrics(case_id: str, posts: Sequence[dict], impacts: Sequence[dict]) -> list[dict]:
    by_account = defaultdict(list)
    preferred = _preferred_horizon_impacts(impacts)
    for impact in preferred:
        by_account[impact["handle"]].append(impact)
    first_posts = sorted(posts, key=lambda item: item["created_at"])
    lead_rank = {post["handle"]: idx for idx, post in enumerate(first_posts)}
    rows = []
    for account, samples in by_account.items():
        hits = sum(1 for row in samples if row.get("is_positive"))
        tradable = sum(1 for row in samples if row.get("tradable_ramp"))
        strong = sum(1 for row in samples if row.get("strong_ramp") or row.get("is_strong"))
        already_volatile = sum(1 for row in samples if row.get("already_hot_penalty"))
        hit_rate = hits / len(samples) if samples else 0
        strong_rate = strong / len(samples) if samples else 0
        already_volatile_rate = already_volatile / len(samples) if samples else 0
        false_fomo = 1 - hit_rate if samples else None
        lead_score = max(0, 25 - lead_rank.get(account, 5) * 5)
        avg_abs = _avg(row.get("max_favorable_delta") for row in samples) or 0
        avg_delay = _avg(row.get("entry_delay_seconds") for row in samples)
        tradable_score = max(
            0.0,
            min(
                100.0,
                hit_rate * 30
                + strong_rate * 25
                + min(25.0, avg_abs * 250)
                + lead_score * 0.35
                - already_volatile_rate * 8,
            ),
        )
        if tradable > 0 and strong > 0:
            status = "volatility_source"
        elif tradable > 0 or hits > 0:
            status = "watch"
        else:
            status = "noise_or_no_volatility"
        rows.append(
            {
                "account": account,
                "case_id": case_id,
                "lead_score": lead_score,
                "impact_score": tradable_score,
                "hit_rate": hit_rate,
                "false_fomo_rate": false_fomo,
                "sample_size": len(samples),
                "ramp_hit_rate": hit_rate,
                "strong_ramp_rate": strong_rate,
                "avg_entry_delay_seconds": avg_delay,
                "avg_max_favorable_delta": avg_abs,
                "avg_max_adverse_delta": _avg(row.get("max_adverse_delta") for row in samples),
                "already_hot_rate": already_volatile_rate,
                "tradable_score": tradable_score,
                "recommended_status": status,
            }
        )
    return sorted(rows, key=lambda item: (item["tradable_score"], item["lead_score"]), reverse=True)


def _preferred_horizon_impacts(impacts: Sequence[dict]) -> list[dict]:
    if any(impact.get("horizon") == "24h" for impact in impacts):
        return [impact for impact in impacts if impact.get("horizon") == "24h"]
    return list(impacts)


def _preferred_micro_impacts(impacts: Sequence[dict]) -> list[dict]:
    for horizon in ("5m", "10m", "1m"):
        rows = [impact for impact in impacts if impact.get("horizon") == horizon]
        if rows:
            return rows
    return list(impacts)


def _dedupe_directional_posts(posts: Sequence[dict], bucket_minutes: int = 60) -> list[dict]:
    seen = set()
    result = []
    for post in sorted(posts, key=lambda item: item["created_at"]):
        if post.get("direction") == "watch_only":
            continue
        ts = to_datetime(post["created_at"])
        if ts is None:
            continue
        bucket = int(ts.timestamp() // (bucket_minutes * 60))
        key = (post["handle"], post["direction"], bucket)
        if key in seen:
            continue
        seen.add(key)
        result.append(post)
    return result


def _dedupe_volatility_posts(posts: Sequence[dict], bucket_minutes: int = 60) -> list[dict]:
    seen = set()
    result = []
    for post in sorted(posts, key=lambda item: item["created_at"]):
        ts = to_datetime(post["created_at"])
        if ts is None:
            continue
        bucket = int(ts.timestamp() // (bucket_minutes * 60))
        key = (post["handle"], bucket)
        if key in seen:
            continue
        seen.add(key)
        result.append(post)
    return result


def _case_posts(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    rows = con.execute(
        """
        select case_id, post_id, handle, created_at, text, direction, matched_keywords
        from event_case_posts
        where case_id = ?
        order by created_at
        """,
        [case_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        item["matched_keywords"] = _loads(item.get("matched_keywords"), [])
        result.append(item)
    return result


def _case_ticks(con: duckdb.DuckDBPyConnection, market_slug: str) -> list[dict]:
    yes_token = _yes_token_id(con, market_slug)
    rows = con.execute(
        """
        select observed_at, market_slug, token_id, mid, spread, liquidity, best_bid, best_ask, tick_source
        from market_ticks
        where market_slug = ? and mid is not null
          and (? is null or token_id = ?)
        order by observed_at
        """,
        [market_slug, yes_token, yes_token],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _yes_token_id(con: duckdb.DuckDBPyConnection, market_slug: str) -> Optional[str]:
    row = con.execute("select clob_token_ids from markets where market_slug = ? limit 1", [market_slug]).fetchone()
    if not row:
        return None
    tokens = _json_list(row[0])
    return tokens[0] if tokens else None


def _event_metrics(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    rows = con.execute(
        """
        select account, lead_score, impact_score, hit_rate, false_fomo_rate, sample_size,
               ramp_hit_rate, strong_ramp_rate, avg_entry_delay_seconds,
               avg_max_favorable_delta, avg_max_adverse_delta, already_hot_rate,
               tradable_score, micro_hit_rate, median_time_to_price_in,
               sub_10m_hit_rate, late_after_price_move_rate, recommended_status
        from event_account_metrics
        where case_id = ?
        order by coalesce(tradable_score, impact_score) desc, lead_score desc
        """,
        [case_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _event_impacts(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    rows = con.execute(
        """
        select post_id, handle, horizon, mode, entry_mid, future_mid, delta, close_delta,
               max_favorable_delta, max_adverse_delta, entry_delay_seconds, price_in_time,
               ramp_duration_minutes, price_move_started_before_post, is_positive,
               is_strong, tradable_ramp, strong_ramp, already_hot_penalty,
               crowded_entry, late_stage_ramp, price_data_resolution,
               time_to_3pp_seconds, max_move_10m, reversal_30m,
               execution_cost, net_close_delta, net_max_favorable_delta,
               net_max_adverse_delta, paper_trade_positive, paper_trade_strong,
               edge_after_cost, reward_to_risk, risk_adjusted_edge,
               risk_tags
        from event_post_impacts
        where case_id = ?
        order by handle, horizon
        """,
        [case_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        item["risk_tags"] = _loads(item.get("risk_tags"), [])
        result.append(item)
    return result


def _live_burst_runs(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    try:
        return live_burst_runs_for_case(con, case_id)
    except duckdb.CatalogException:
        return []


def _best_live_micro_row(rows: Sequence[dict]) -> Optional[dict]:
    if not rows:
        return None
    for horizon in ("10m", "5m", "1m", "30m", "2h"):
        for row in rows:
            if row.get("horizon") == horizon:
                return row
    return rows[0]


def _row_move(row: Optional[dict]) -> Optional[float]:
    if not row:
        return None
    return row.get("close_delta") if row.get("close_delta") is not None else row.get("delta")


def _row_net_move(row: Optional[dict]) -> Optional[float]:
    if not row:
        return None
    return row.get("net_close_delta") if row.get("net_close_delta") is not None else _row_move(row)


def _nearest_tick(ticks: Sequence[dict], target: datetime, before: bool) -> Optional[dict]:
    candidates = [tick for tick in ticks if to_datetime(tick["observed_at"]) is not None]
    if before:
        candidates = [tick for tick in candidates if to_datetime(tick["observed_at"]) <= target]
        return candidates[-1] if candidates else None
    candidates = [tick for tick in candidates if to_datetime(tick["observed_at"]) >= target]
    return candidates[0] if candidates else None


def _pre_move(ticks: Sequence[dict], post_ts: datetime, entry_mid: float, sign: int) -> float:
    start = post_ts - timedelta(hours=6)
    previous = [tick for tick in ticks if _between_tick(tick, start, post_ts)]
    if not previous:
        return 0
    return (entry_mid - float(previous[0]["mid"])) * sign


def _pre_move_before_post(ticks: Sequence[dict], post_ts: datetime, sign: int) -> float:
    start = post_ts - timedelta(hours=6)
    previous = [tick for tick in ticks if _between_tick(tick, start, post_ts)]
    if len(previous) < 2:
        return 0
    return (float(previous[-1]["mid"]) - float(previous[0]["mid"])) * sign


def _pre_abs_move_before_post(ticks: Sequence[dict], post_ts: datetime) -> float:
    start = post_ts - timedelta(hours=6)
    previous = [tick for tick in ticks if _between_tick(tick, start, post_ts)]
    if len(previous) < 2:
        return 0
    return abs(float(previous[-1]["mid"]) - float(previous[0]["mid"]))


def _between_tick(tick: Mapping[str, object], start: datetime, end: datetime) -> bool:
    ts = to_datetime(tick.get("observed_at"))
    return ts is not None and start <= ts <= end


def _micro_impact_row(
    case_id: str,
    market_slug: str,
    post: Mapping[str, object],
    horizon: str,
    entry_mid: Optional[float],
    future_mid: Optional[float],
    close_delta: Optional[float],
    max_favorable: Optional[float],
    max_adverse: Optional[float],
    entry_delay: Optional[float],
    price_in_time: Optional[str],
    time_to_3pp: Optional[float],
    max_10m: Optional[float],
    late_after_price_move: bool,
    micro_hit: bool,
    strong_micro: bool,
    crowded: bool,
    resolution_label: str,
    risk_tags: Sequence[str],
    tradable: bool = False,
    reversal_30m: Optional[float] = None,
    execution_cost: Optional[float] = None,
    net_close_delta: Optional[float] = None,
    net_max_favorable: Optional[float] = None,
    net_max_adverse: Optional[float] = None,
    edge_after_cost: Optional[float] = None,
    reward_to_risk: Optional[float] = None,
    risk_adjusted_edge: Optional[float] = None,
    paper_trade_positive: bool = False,
    paper_trade_strong: bool = False,
) -> dict:
    return {
        "mode": "micro",
        "case_id": case_id,
        "post_id": post["post_id"],
        "handle": post["handle"],
        "market_slug": market_slug,
        "horizon": horizon,
        "entry_mid": entry_mid,
        "future_mid": future_mid,
        "delta": close_delta,
        "close_delta": close_delta,
        "max_favorable_delta": max_favorable,
        "max_adverse_delta": max_adverse,
        "entry_delay_seconds": entry_delay,
        "price_in_time": price_in_time,
        "ramp_duration_minutes": (time_to_3pp / 60) if time_to_3pp is not None else None,
        "price_move_started_before_post": late_after_price_move,
        "is_positive": micro_hit,
        "is_strong": strong_micro,
        "tradable_ramp": tradable,
        "strong_ramp": strong_micro,
        "already_hot_penalty": late_after_price_move,
        "crowded_entry": crowded,
        "late_stage_ramp": crowded and micro_hit,
        "price_data_resolution": resolution_label,
        "time_to_3pp_seconds": time_to_3pp,
        "max_move_10m": max_10m,
        "reversal_30m": reversal_30m,
        "execution_cost": execution_cost,
        "net_close_delta": net_close_delta,
        "net_max_favorable_delta": net_max_favorable,
        "net_max_adverse_delta": net_max_adverse,
        "edge_after_cost": edge_after_cost,
        "reward_to_risk": reward_to_risk,
        "risk_adjusted_edge": risk_adjusted_edge,
        "paper_trade_positive": paper_trade_positive,
        "paper_trade_strong": paper_trade_strong,
        "risk_tags": list(dict.fromkeys(risk_tags)),
    }


def _tick_resolution(ticks: Sequence[dict], start: datetime, end: datetime) -> tuple[str, Optional[float]]:
    stamps = [to_datetime(tick.get("observed_at")) for tick in ticks if _between_tick(tick, start, end)]
    stamps = [stamp for stamp in stamps if stamp is not None]
    if len(stamps) < 2:
        return "sparse", None
    gaps = sorted((stamps[index] - stamps[index - 1]).total_seconds() for index in range(1, len(stamps)))
    median_gap = gaps[len(gaps) // 2]
    sources = {str(tick.get("tick_source") or "") for tick in ticks if _between_tick(tick, start, end)}
    if median_gap <= 1.5:
        return "1s", median_gap
    if median_gap <= 10:
        return "10s", median_gap
    if median_gap <= 30:
        return "30s", median_gap
    if median_gap <= 75:
        return ("minute_floor" if any(source.startswith("historical") for source in sources) else "1m", median_gap)
    return "sparse", median_gap


def _signed_or_abs_move(raw_move: float, sign: int) -> float:
    return abs(raw_move) if sign == 0 else raw_move * sign


def _round_trip_execution_cost(entry_tick: Mapping[str, object], exit_tick: Optional[Mapping[str, object]]) -> float:
    entry_half = _half_spread(entry_tick)
    exit_half = _half_spread(exit_tick) if exit_tick is not None else None
    if entry_half is None and exit_half is None:
        return PAPER_TRADE_MIN_ROUND_TRIP_COST
    known_cost = sum(value for value in (entry_half, exit_half) if value is not None)
    missing_halves = int(entry_half is None) + int(exit_half is None)
    fallback_cost = missing_halves * (PAPER_TRADE_MIN_ROUND_TRIP_COST / 2)
    return max(PAPER_TRADE_MIN_ROUND_TRIP_COST, known_cost + fallback_cost + PAPER_TRADE_SLIPPAGE_BUFFER)


def _reward_to_risk(net_max_favorable: Optional[float], net_max_adverse: Optional[float], execution_cost: Optional[float]) -> Optional[float]:
    if net_max_favorable is None or net_max_favorable <= 0:
        return None
    adverse = abs(min(0.0, float(net_max_adverse or 0.0)))
    denominator = max(adverse, float(execution_cost or 0.0), PAPER_TRADE_MIN_ROUND_TRIP_COST / 2)
    return net_max_favorable / denominator


def _risk_adjusted_edge(net_max_favorable: Optional[float], net_max_adverse: Optional[float]) -> Optional[float]:
    if net_max_favorable is None or net_max_adverse is None:
        return None
    return float(net_max_favorable) + min(0.0, float(net_max_adverse))


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


def _float_value(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_threshold_tick(ticks: Sequence[dict], entry_mid: float, sign: int, threshold: float) -> Optional[dict]:
    for tick in ticks:
        if tick.get("mid") is None:
            continue
        if _signed_or_abs_move(float(tick["mid"]) - entry_mid, sign) >= threshold:
            return tick
    return None


def _merge_time_windows(windows: Sequence[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    ordered = sorted((start, end) for start, end in windows if start <= end)
    if not ordered:
        return []
    merged = [ordered[0]]
    for start, end in ordered[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def _point_timestamp(point: Mapping[str, object]) -> Optional[int]:
    for key in ("t", "timestamp", "time", "ts"):
        value = point.get(key)
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            parsed = to_datetime(value)
            return int(parsed.timestamp()) if parsed else None
        if number > 10_000_000_000:
            number = number / 1000
        return int(number)
    return None


def _point_price(point: Mapping[str, object]) -> Optional[float]:
    for key in ("p", "price", "mid", "value"):
        value = point.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
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


def _loads(value: object, default):
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _looks_like_trump_china_case(keywords: Sequence[str]) -> bool:
    lowered = {str(keyword).lower() for keyword in keywords}
    return "trump" in lowered and "china" in lowered


def _trump_china_direct_match(lowered_text: str) -> bool:
    trump_terms = ("trump", "donald trump", "特朗普")
    china_terms = ("china", "beijing", "xi", "中国", "中美", "北京", "习近平")
    action_terms = TRUMP_CHINA_BULLISH + TRUMP_CHINA_BEARISH
    has_trump = any(_keyword_in_text(term, lowered_text) for term in trump_terms)
    has_china = any(_keyword_in_text(term, lowered_text) for term in china_terms)
    has_action = any(_keyword_in_text(term, lowered_text) for term in action_terms)
    return has_trump and has_china and has_action


def _trump_china_catalyst_match(lowered_text: str) -> bool:
    anchor_terms = ("trump", "donald trump", "potus", "white house", "us official", "u.s. official", "特朗普", "白宫")
    catalyst_terms = TRUMP_CHINA_CATALYSTS
    impact_terms = TRUMP_CHINA_CATALYST_BULLISH + TRUMP_CHINA_CATALYST_BEARISH + TRUMP_CHINA_CATALYST_WATCH + TRUMP_CHINA_BEARISH
    has_anchor = any(_keyword_in_text(term, lowered_text) for term in anchor_terms)
    has_catalyst = any(_keyword_in_text(term, lowered_text) for term in catalyst_terms)
    has_impact = any(_keyword_in_text(term, lowered_text) for term in impact_terms)
    return has_anchor and has_catalyst and has_impact


def _keyword_in_text(keyword: str, lowered_text: str) -> bool:
    keyword = keyword.lower().strip()
    if not keyword:
        return False
    if keyword.isascii() and re.fullmatch(r"[a-z0-9_]+", keyword):
        return re.search(rf"(?<![a-z0-9_]){re.escape(keyword)}(?![a-z0-9_])", lowered_text) is not None
    return keyword in lowered_text


def _avg(values: Iterable[object]) -> Optional[float]:
    floats = []
    for value in values:
        if value is None:
            continue
        try:
            floats.append(float(value))
        except (TypeError, ValueError):
            continue
    if not floats:
        return None
    return sum(floats) / len(floats)


def _median(values: Iterable[object]) -> Optional[float]:
    floats = []
    for value in values:
        if value is None:
            continue
        try:
            floats.append(float(value))
        except (TypeError, ValueError):
            continue
    if not floats:
        return None
    floats.sort()
    midpoint = len(floats) // 2
    if len(floats) % 2:
        return floats[midpoint]
    return (floats[midpoint - 1] + floats[midpoint]) / 2


def _fmt(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)
