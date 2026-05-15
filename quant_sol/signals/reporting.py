from __future__ import annotations

import json
from pathlib import Path
from typing import List

import duckdb

from .config import SIGNAL_REPORT_ROOT


def write_signal_report(con: duckdb.DuckDBPyConnection, date: str, report_root: Path = SIGNAL_REPORT_ROOT) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    path = report_root / f"signal_report_{date}.md"
    rows = _signals_for_date(con, date)
    lines = [
        f"# FOMO Premium Signal Report {date}",
        "",
        "## Top FOMO Divergence Signals",
        "",
        "| Score | Market | Narrative | Mid | 6h move | Deadline | FOMO capacity | Risk tags |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    if not rows:
        lines.append("| n/a | No signals | n/a | n/a | n/a | n/a | n/a | n/a |")
    for row in rows:
        risk_tags = ", ".join(_loads(row["risk_tags"], []))
        price = _loads(row["price_window"], {})
        lines.append(
            f"| {row['score']} | `{row['market_slug']}` | {price.get('narrative_direction')} | "
            f"{_fmt(price.get('current_market_probability'))} | {_fmt(price.get('market_move_6h'))} | "
            f"{_fmt(price.get('deadline_days'))} | {_fmt(price.get('fomo_capacity'))} | {risk_tags or 'none'} |"
        )

    lines.extend(["", "## Evidence", ""])
    for row in rows:
        lines.extend(_signal_detail(row))

    lines.extend(
        [
            "",
            "## Review Queues",
            "",
            "- False FOMO candidates: high social velocity but no later price convergence.",
            "- Already-priced-in cases: strong narrative where 6h/24h move already exceeded thresholds.",
            "- Near-deadline rejects: strong narrative that is too close to deterministic resolution.",
            "- Watchlist gaps: high market movement with no matching narrative snapshot.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _signals_for_date(con: duckdb.DuckDBPyConnection, date: str) -> List[dict]:
    rows = con.execute(
        """
        select signal_id, generated_at, event_family, market_slug, direction_hint, score,
               confidence, evidence, risk_tags, source_posts, wallet_flows, price_window
        from signal_events
        where cast(generated_at as date) = cast(? as date)
        order by score desc, generated_at desc
        """,
        [date],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _signal_detail(row: dict) -> List[str]:
    evidence = _loads(row["evidence"], {})
    source_posts = _loads(row["source_posts"], [])
    wallet_flows = _loads(row["wallet_flows"], [])
    price_window = _loads(row["price_window"], {})
    post = source_posts[0] if source_posts else {}
    lines = [
        f"### {row['market_slug']}",
        "",
        f"- Score/confidence: {row['score']} / {row['confidence']}",
        f"- Generated at: {row['generated_at']}",
        f"- Source: @{post.get('handle', '')} {post.get('created_at', '')} {post.get('url', '')}",
        f"- FOMO state: mid={price_window.get('current_market_probability')}, "
        f"direction={price_window.get('narrative_direction')}, velocity={price_window.get('narrative_velocity')}, "
        f"capacity={price_window.get('fomo_capacity')}, confirmation={price_window.get('confirmation_status')}",
        f"- Evidence: source_quality={evidence.get('source_quality_score')}, velocity={evidence.get('social_velocity_score')}, "
        f"acceleration={evidence.get('narrative_acceleration_score')}, inertia={evidence.get('market_inertia_score')}, "
        f"capacity={evidence.get('fomo_capacity_score')}, liquidity={evidence.get('liquidity_executability_score')}, "
        f"wallet={evidence.get('early_wallet_confirmation_score')}, penalty={evidence.get('anti_front_run_penalty')}",
        f"- Market moves: 1h={price_window.get('market_move_1h')}, 6h={price_window.get('market_move_6h')}, "
        f"24h={price_window.get('market_move_24h')}, spread={price_window.get('spread')}, liquidity={price_window.get('liquidity')}",
    ]
    if wallet_flows:
        lines.append("- Wallet flows:")
        for flow in wallet_flows[:5]:
            lines.append(
                f"  - {flow.get('wallet')} {flow.get('side')} "
                f"${float(flow.get('notional') or 0):,.0f} at {flow.get('activity_ts')}"
            )
    lines.append("")
    return lines


def _loads(value: object, default):
    if isinstance(value, (list, dict)):
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
