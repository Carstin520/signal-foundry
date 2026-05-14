import json
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

import duckdb

from .config import WATCHLIST, WalletConfig
from .models import WalletMetrics
from .storage import utc_now_iso


def analyze_wallets(con: duckdb.DuckDBPyConnection, labels: Iterable[str]) -> List[WalletMetrics]:
    analyzed_at = utc_now_iso()
    metrics = [_analyze_wallet(con, WATCHLIST[label], analyzed_at) for label in labels]
    con.executemany(
        """
        insert into wallet_metrics
        (wallet, analyzed_at, address, status, realized_pnl, unrealized_pnl, total_pnl,
         win_rate, closed_markets, open_markets, total_volume, top_category,
         top_category_pnl_share, max_market_pnl, max_market_pnl_share, late_entry_ratio,
         confidence, risk_tags)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [_metrics_row(metric) for metric in metrics],
    )
    return metrics


def _analyze_wallet(
    con: duckdb.DuckDBPyConnection,
    wallet: WalletConfig,
    analyzed_at: str,
) -> WalletMetrics:
    if not wallet.is_resolved:
        return WalletMetrics(
            label=wallet.label,
            address=wallet.address,
            status=wallet.status,
            analyzed_at=analyzed_at,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            total_pnl=0.0,
            win_rate=None,
            closed_markets=0,
            open_markets=0,
            total_volume=0.0,
            top_category=None,
            top_category_pnl_share=None,
            max_market_pnl=0.0,
            max_market_pnl_share=None,
            late_entry_ratio=None,
            confidence="unresolved_wallet",
            risk_tags=["unresolved_wallet"],
        )

    realized_pnl = _scalar(con, "select coalesce(sum(pnl), 0) from closed_positions where wallet = ?", [wallet.label])
    unrealized_pnl = _scalar(con, "select coalesce(sum(pnl), 0) from positions where wallet = ?", [wallet.label])
    closed_markets = int(
        _scalar(
            con,
            "select count(distinct coalesce(market_id, market)) from closed_positions where wallet = ?",
            [wallet.label],
        )
    )
    open_markets = int(
        _scalar(
            con,
            "select count(distinct coalesce(market_id, market)) from positions where wallet = ?",
            [wallet.label],
        )
    )
    total_volume = _scalar(con, "select coalesce(sum(volume), 0) from closed_positions where wallet = ?", [wallet.label])
    win_rate = _win_rate(con, wallet.label)
    top_category, top_category_pnl_share = _top_category(con, wallet.label, realized_pnl)
    max_market_pnl, max_market_pnl_share = _max_market_contribution(con, wallet.label, realized_pnl)
    late_entry_ratio = _late_entry_ratio(con, wallet.label)
    total_pnl = realized_pnl + unrealized_pnl
    confidence = _confidence(closed_markets, open_markets, realized_pnl, unrealized_pnl)
    risk_tags = _risk_tags(
        closed_markets=closed_markets,
        open_markets=open_markets,
        total_volume=total_volume,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        top_category_pnl_share=top_category_pnl_share,
        max_market_pnl_share=max_market_pnl_share,
        late_entry_ratio=late_entry_ratio,
    )

    return WalletMetrics(
        label=wallet.label,
        address=wallet.address,
        status=wallet.status,
        analyzed_at=analyzed_at,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        total_pnl=total_pnl,
        win_rate=win_rate,
        closed_markets=closed_markets,
        open_markets=open_markets,
        total_volume=total_volume,
        top_category=top_category,
        top_category_pnl_share=top_category_pnl_share,
        max_market_pnl=max_market_pnl,
        max_market_pnl_share=max_market_pnl_share,
        late_entry_ratio=late_entry_ratio,
        confidence=confidence,
        risk_tags=risk_tags,
    )


def _win_rate(con: duckdb.DuckDBPyConnection, wallet_label: str) -> Optional[float]:
    row = con.execute(
        """
        with per_market as (
            select coalesce(market_id, market) as market_key, sum(coalesce(pnl, 0)) as market_pnl
            from closed_positions
            where wallet = ?
            group by 1
        )
        select count(*) as n, sum(case when market_pnl > 0 then 1 else 0 end) as wins
        from per_market
        """,
        [wallet_label],
    ).fetchone()
    if not row or row[0] == 0:
        return None
    return float(row[1] or 0) / float(row[0])


def _top_category(con: duckdb.DuckDBPyConnection, wallet_label: str, realized_pnl: float) -> Sequence[Optional[float]]:
    row = con.execute(
        """
        select coalesce(category, 'unknown') as category, sum(coalesce(pnl, 0)) as category_pnl
        from closed_positions
        where wallet = ?
        group by 1
        order by abs(category_pnl) desc
        limit 1
        """,
        [wallet_label],
    ).fetchone()
    if not row:
        return None, None
    share = _safe_share(abs(float(row[1] or 0)), abs(realized_pnl))
    return row[0], share


def _max_market_contribution(
    con: duckdb.DuckDBPyConnection,
    wallet_label: str,
    realized_pnl: float,
) -> Sequence[Optional[float]]:
    row = con.execute(
        """
        select sum(coalesce(pnl, 0)) as market_pnl
        from closed_positions
        where wallet = ?
        group by coalesce(market_id, market)
        order by abs(market_pnl) desc
        limit 1
        """,
        [wallet_label],
    ).fetchone()
    if not row:
        return 0.0, None
    max_market_pnl = float(row[0] or 0)
    return max_market_pnl, _safe_share(abs(max_market_pnl), abs(realized_pnl))


def _late_entry_ratio(con: duckdb.DuckDBPyConnection, wallet_label: str) -> Optional[float]:
    rows = con.execute(
        "select raw_json from activity where wallet = ?",
        [wallet_label],
    ).fetchall()
    total = 0
    late = 0
    for (raw_json,) in rows:
        try:
            item = json.loads(raw_json)
        except (TypeError, json.JSONDecodeError):
            continue
        activity_ts = _extract_datetime(item, ["timestamp", "createdAt", "created_at", "time"])
        end_ts = _extract_datetime(item, ["endDate", "end_date", "marketEndDate", "market_end_date", "closeTime"])
        size = _extract_float(item, ["size", "shares", "quantity", "amount"]) or 0.0
        if activity_ts is None or end_ts is None or size <= 0:
            continue
        total += 1
        seconds_to_end = (end_ts - activity_ts).total_seconds()
        if 0 <= seconds_to_end <= 24 * 60 * 60:
            late += 1
    if total == 0:
        return None
    return late / total


def _confidence(closed_markets: int, open_markets: int, realized_pnl: float, unrealized_pnl: float) -> str:
    if closed_markets == 0 and open_markets == 0:
        return "no_data"
    if closed_markets == 0:
        return "low_confidence"
    if abs(unrealized_pnl) > abs(realized_pnl) and abs(unrealized_pnl) > 100:
        return "mixed_realized_unrealized"
    return "ok"


def _risk_tags(
    closed_markets: int,
    open_markets: int,
    total_volume: float,
    realized_pnl: float,
    unrealized_pnl: float,
    top_category_pnl_share: Optional[float],
    max_market_pnl_share: Optional[float],
    late_entry_ratio: Optional[float],
) -> List[str]:
    tags: List[str] = []
    if max_market_pnl_share is not None and max_market_pnl_share >= 0.5:
        tags.append("concentrated")
    if closed_markets + open_markets <= 5 and total_volume >= 100_000:
        tags.append("new_wallet_large_size")
    if abs(unrealized_pnl) > abs(realized_pnl) and abs(unrealized_pnl) >= 100:
        tags.append("unrealized_heavy")
    if top_category_pnl_share is not None and top_category_pnl_share >= 0.7:
        tags.append("category_specialist")
    if late_entry_ratio is not None and late_entry_ratio >= 0.3:
        tags.append("late_entry")
    return tags


def _scalar(con: duckdb.DuckDBPyConnection, query: str, params: Sequence[object]) -> float:
    row = con.execute(query, params).fetchone()
    if row is None or row[0] is None:
        return 0.0
    return float(row[0])


def _safe_share(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def _extract_float(item: Dict[str, object], keys: Iterable[str]) -> Optional[float]:
    for key in keys:
        value = item.get(key)
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _extract_datetime(item: Dict[str, object], keys: Iterable[str]) -> Optional[datetime]:
    for key in keys:
        value = item.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 10_000_000_000:
                ts = ts / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(value, str):
            normalized = value.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
    return None


def _metrics_row(metric: WalletMetrics) -> Sequence[object]:
    return (
        metric.label,
        metric.analyzed_at,
        metric.address,
        metric.status,
        metric.realized_pnl,
        metric.unrealized_pnl,
        metric.total_pnl,
        metric.win_rate,
        metric.closed_markets,
        metric.open_markets,
        metric.total_volume,
        metric.top_category,
        metric.top_category_pnl_share,
        metric.max_market_pnl,
        metric.max_market_pnl_share,
        metric.late_entry_ratio,
        metric.confidence,
        json.dumps(metric.risk_tags),
    )

