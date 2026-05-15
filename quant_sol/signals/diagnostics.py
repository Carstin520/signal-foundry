from __future__ import annotations

from pathlib import Path
from typing import Mapping, Optional

import duckdb

from .config import SIGNAL_REPORT_ROOT
from .utils import utc_now_iso


CORE_TABLES = (
    "markets",
    "market_ticks",
    "x_accounts",
    "x_posts",
    "x_follow_graph",
    "account_market_mentions",
    "account_market_outcomes",
    "account_impact_metrics",
    "signal_events",
    "signal_outcomes",
    "wallet_activity",
)


def model_diagnostics(con: duckdb.DuckDBPyConnection) -> dict:
    counts = {table: _count(con, table) for table in CORE_TABLES}
    status_counts = _account_status_counts(con)
    tick_coverage = _tick_coverage(con)
    signal_coverage = _signal_coverage(con)
    blockers = _blockers(counts, tick_coverage, signal_coverage)
    return {
        "generated_at": utc_now_iso(),
        "counts": counts,
        "account_status_counts": status_counts,
        "tick_coverage": tick_coverage,
        "signal_coverage": signal_coverage,
        "blockers": blockers,
        "round_status": _round_status(counts, tick_coverage, signal_coverage),
    }


def write_model_diagnostics(
    con: duckdb.DuckDBPyConnection,
    report_root: Path = SIGNAL_REPORT_ROOT,
    date: Optional[str] = None,
) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    diagnostics = model_diagnostics(con)
    suffix = date or diagnostics["generated_at"][:10]
    path = report_root / f"model_diagnostics_{suffix}.md"
    path.write_text(_render_markdown(diagnostics), encoding="utf-8")
    return path


def _render_markdown(diagnostics: Mapping[str, object]) -> str:
    counts = diagnostics["counts"]
    tick = diagnostics["tick_coverage"]
    signal = diagnostics["signal_coverage"]
    status_counts = diagnostics["account_status_counts"]
    lines = [
        "# Signal Foundry Model Diagnostics",
        "",
        f"- Generated at: {diagnostics['generated_at']}",
        "",
        "## System Status",
        "",
        "| Area | Status | Detail |",
        "| --- | --- | --- |",
    ]
    for row in diagnostics["round_status"]:
        lines.append(f"| {row['area']} | {row['status']} | {row['detail']} |")

    lines.extend(["", "## Data Coverage", "", "| Table | Rows |", "| --- | ---: |"])
    for table, count in counts.items():
        lines.append(f"| `{table}` | {count} |")

    lines.extend(
        [
            "",
            "## Account Ranking Status",
            "",
            "| Status | Accounts |",
            "| --- | ---: |",
        ]
    )
    if status_counts:
        for status, count in status_counts.items():
            lines.append(f"| `{status}` | {count} |")
    else:
        lines.append("| `none` | 0 |")

    lines.extend(
        [
            "",
            "## Market Tick Coverage",
            "",
            f"- Markets with ticks: {tick['markets_with_ticks']}",
            f"- Markets with 2+ ticks: {tick['markets_with_two_or_more_ticks']}",
            f"- Average ticks per ticked market: {tick['avg_ticks_per_ticked_market']:.2f}",
            "",
            "## Signal Coverage",
            "",
            f"- Generated signal events: {signal['signals']}",
            f"- Evaluated signal outcomes: {signal['outcomes']}",
            f"- Account market outcomes: {counts['account_market_outcomes']}",
            "",
            "## Blockers",
            "",
        ]
    )
    blockers = diagnostics["blockers"]
    if blockers:
        lines.extend(f"- `{blocker}`" for blocker in blockers)
    else:
        lines.append("- None.")
    return "\n".join(lines)


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    row = con.execute(f"select count(*) from {table}").fetchone()
    return int(row[0] if row else 0)


def _account_status_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    rows = con.execute(
        """
        select recommended_status, count(*)
        from account_impact_metrics
        group by recommended_status
        order by recommended_status
        """
    ).fetchall()
    return {str(status or "unknown"): int(count) for status, count in rows}


def _tick_coverage(con: duckdb.DuckDBPyConnection) -> dict[str, float | int]:
    rows = con.execute(
        """
        select market_slug, count(*) as tick_count
        from market_ticks
        where market_slug is not null and mid is not null
        group by market_slug
        """
    ).fetchall()
    tick_counts = [int(row[1]) for row in rows]
    return {
        "markets_with_ticks": len(tick_counts),
        "markets_with_two_or_more_ticks": sum(1 for count in tick_counts if count >= 2),
        "avg_ticks_per_ticked_market": sum(tick_counts) / len(tick_counts) if tick_counts else 0.0,
    }


def _signal_coverage(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        "signals": _count(con, "signal_events"),
        "outcomes": _count(con, "signal_outcomes"),
    }


def _blockers(counts: Mapping[str, int], tick: Mapping[str, float | int], signal: Mapping[str, int]) -> list[str]:
    blockers = []
    if counts["x_posts"] == 0:
        blockers.append("no_x_posts")
    if counts["markets"] == 0:
        blockers.append("no_markets")
    if counts["market_ticks"] == 0:
        blockers.append("no_market_ticks")
    if counts["account_market_mentions"] > 0 and counts["account_market_outcomes"] == 0:
        blockers.append("matched_posts_without_price_outcomes")
    if tick["markets_with_two_or_more_ticks"] == 0 and counts["account_market_mentions"] > 0:
        blockers.append("insufficient_tick_history_for_backtest")
    if signal["signals"] == 0:
        blockers.append("no_market_level_signal_events")
    if counts["x_follow_graph"] == 0:
        blockers.append("no_follow_graph_source_chain")
    return blockers


def _round_status(
    counts: Mapping[str, int],
    tick: Mapping[str, float | int],
    signal: Mapping[str, int],
) -> list[dict[str, str]]:
    return [
        {
            "area": "Round 1 account ranking guardrails",
            "status": "done" if counts["account_impact_metrics"] > 0 else "missing",
            "detail": "confirmation sources, insufficient data statuses, and account metrics are present.",
        },
        {
            "area": "Round 2 market impact plumbing",
            "status": "partial" if counts["account_market_mentions"] > 0 else "missing",
            "detail": f"{counts['account_market_mentions']} post-market links; {counts['account_market_outcomes']} evaluated account outcomes.",
        },
        {
            "area": "Round 2 tick history",
            "status": "done" if tick["markets_with_two_or_more_ticks"] > 0 else "partial",
            "detail": f"{tick['markets_with_two_or_more_ticks']} markets have at least two midpoint observations.",
        },
        {
            "area": "Round 3 market-level signal loop",
            "status": "done" if signal["signals"] > 0 and signal["outcomes"] > 0 else "partial",
            "detail": f"{signal['signals']} signal events and {signal['outcomes']} signal outcomes.",
        },
    ]
