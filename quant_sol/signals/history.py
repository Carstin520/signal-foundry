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
DEFAULT_HORIZONS = ("1h", "6h", "24h", "72h")


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
                    "tick_source": "historical",
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
) -> tuple[list[dict], list[dict]]:
    case = get_event_case(con, case_id)
    if not case or not case.get("market_slug"):
        return [], []
    posts = _case_posts(con, case_id)
    ticks = _case_ticks(con, str(case["market_slug"]))
    directional_posts = _dedupe_directional_posts(posts)
    impacts = _post_impacts(case_id, str(case["market_slug"]), directional_posts, ticks, horizons)
    metrics = _account_metrics(case_id, directional_posts, impacts)
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
    lines = [
        f"# Event Backtest: {case_id}",
        "",
        f"- Query: {case.get('query') if case else 'unknown'}",
        f"- Market: `{case.get('market_slug') if case else 'unknown'}`",
        f"- Window: {case.get('start_at') if case else 'n/a'} to {case.get('end_at') if case else 'n/a'}",
        f"- Posts: {len(posts)}",
        f"- Impacts: {len(impacts)}",
        "",
        "## Account Ranking",
        "",
        "| Account | Lead | Impact | Hit Rate | False FOMO | Samples | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    if not metrics:
        lines.append("| n/a | 0 | 0 | n/a | n/a | 0 | no_samples |")
    for row in metrics:
        lines.append(
            f"| @{row['account']} | {_fmt(row['lead_score'])} | {_fmt(row['impact_score'])} | "
            f"{_fmt(row['hit_rate'])} | {_fmt(row['false_fomo_rate'])} | {row['sample_size']} | {row['recommended_status']} |"
        )
    lines.extend(
        [
            "",
            "## Post Evidence",
            "",
            "| Time | Account | Direction | Text |",
            "| --- | --- | --- | --- |",
        ]
    )
    for post in posts[:50]:
        text = " ".join(str(post.get("text") or "").split())[:160]
        lines.append(f"| {post['created_at']} | @{post['handle']} | {post['direction']} | {text} |")
    lines.extend(
        [
            "",
            "## Price Impact Samples",
            "",
            "| Account | Horizon | Entry | Future | Delta | Favorable | Adverse | Late To Price | Positive |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in impacts[:80]:
        lines.append(
            f"| @{row['handle']} | {row['horizon']} | {_fmt(row['entry_mid'])} | {_fmt(row['future_mid'])} | "
            f"{_fmt(row['delta'])} | {_fmt(row['max_favorable_delta'])} | {_fmt(row['max_adverse_delta'])} | "
            f"{row['price_move_started_before_post']} | {row['is_positive']} |"
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
        base.extend(["china", "trump", "中国"])
    return list(dict.fromkeys(term.lower() for term in base if term))


def matched_keywords(text: str, keywords: Sequence[str]) -> list[str]:
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in lowered]


def direction_from_text(text: str) -> str:
    lowered = text.lower()
    bull = sum(1 for term in TRUMP_CHINA_BULLISH if term.lower() in lowered)
    bear = sum(1 for term in TRUMP_CHINA_BEARISH if term.lower() in lowered)
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "watch_only"


def x_case_query(handle: str, keywords: Sequence[str]) -> str:
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


def _post_impacts(case_id: str, market_slug: str, posts: Sequence[dict], ticks: Sequence[dict], horizons: Sequence[str]) -> list[dict]:
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
                    "case_id": case_id,
                    "post_id": post["post_id"],
                    "handle": post["handle"],
                    "market_slug": market_slug,
                    "horizon": horizon,
                    "entry_mid": entry_mid,
                    "future_mid": future_mid,
                    "delta": delta,
                    "max_favorable_delta": max(signed_moves),
                    "max_adverse_delta": min(signed_moves),
                    "price_move_started_before_post": late_to_price,
                    "is_positive": (not late_to_price) and (delta >= 0.03 or max(signed_moves) >= 0.03),
                    "is_strong": (not late_to_price) and max(signed_moves) >= 0.08 and min(signed_moves) > -0.05,
                }
            )
    return impacts


def _account_metrics(case_id: str, posts: Sequence[dict], impacts: Sequence[dict]) -> list[dict]:
    by_account = defaultdict(list)
    for impact in impacts:
        if impact["horizon"] == "24h":
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
                "recommended_status": status,
            }
        )
    return sorted(rows, key=lambda item: (item["impact_score"], item["lead_score"]), reverse=True)


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
    rows = con.execute(
        """
        select observed_at, market_slug, token_id, mid
        from market_ticks
        where market_slug = ? and mid is not null
        order by observed_at
        """,
        [market_slug],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _event_metrics(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    rows = con.execute(
        """
        select account, lead_score, impact_score, hit_rate, false_fomo_rate, sample_size, recommended_status
        from event_account_metrics
        where case_id = ?
        order by impact_score desc, lead_score desc
        """,
        [case_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _event_impacts(con: duckdb.DuckDBPyConnection, case_id: str) -> list[dict]:
    rows = con.execute(
        """
        select post_id, handle, horizon, entry_mid, future_mid, delta, max_favorable_delta,
               max_adverse_delta, price_move_started_before_post, is_positive
        from event_post_impacts
        where case_id = ?
        order by handle, horizon
        """,
        [case_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


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


def _between_tick(tick: Mapping[str, object], start: datetime, end: datetime) -> bool:
    ts = to_datetime(tick.get("observed_at"))
    return ts is not None and start <= ts <= end


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


def _fmt(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)
