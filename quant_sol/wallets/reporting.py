import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, Optional

from rich.console import Console
from rich.table import Table

from .storage import ensure_report_root


def render_console_report(metrics: Iterable[Mapping[str, object]], console: Optional[Console] = None) -> None:
    console = console or Console()
    table = Table(title="Polymarket Wallet Metrics")
    table.add_column("Wallet")
    table.add_column("Status")
    table.add_column("Realized PnL", justify="right")
    table.add_column("Unrealized PnL", justify="right")
    table.add_column("Win Rate", justify="right")
    table.add_column("Top Category")
    table.add_column("Risk Tags")
    table.add_column("Confidence")

    for row in metrics:
        table.add_row(
            str(row.get("wallet") or ""),
            str(row.get("status") or ""),
            _money(row.get("realized_pnl")),
            _money(row.get("unrealized_pnl")),
            _pct(row.get("win_rate")),
            str(row.get("top_category") or "-"),
            ", ".join(_tags(row.get("risk_tags"))) or "-",
            str(row.get("confidence") or ""),
        )
    console.print(table)


def write_markdown_report(metrics: List[Mapping[str, object]], report_root: Optional[Path] = None) -> Path:
    report_root = report_root or ensure_report_root()
    ts = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")
    path = report_root / f"wallet_report_{ts}.md"
    lines = [
        "# Polymarket Wallet Metrics",
        "",
        f"Generated: {ts}",
        "",
        "| Wallet | Status | Realized PnL | Unrealized PnL | Win Rate | Top Category | Risk Tags | Confidence |",
        "| --- | --- | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for row in metrics:
        lines.append(
            "| {wallet} | {status} | {realized} | {unrealized} | {win_rate} | {category} | {tags} | {confidence} |".format(
                wallet=row.get("wallet") or "",
                status=row.get("status") or "",
                realized=_money(row.get("realized_pnl")),
                unrealized=_money(row.get("unrealized_pnl")),
                win_rate=_pct(row.get("win_rate")),
                category=row.get("top_category") or "-",
                tags=", ".join(_tags(row.get("risk_tags"))) or "-",
                confidence=row.get("confidence") or "",
            )
        )
    lines.extend(
        [
            "",
            "Notes:",
            "- Metrics are recomputed from public read-only Polymarket data stored locally.",
            "- Open positions are not counted as wins or losses.",
            "- Risk tags are screening hints, not proof of non-public information.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _money(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "-"


def _pct(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def _tags(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value] if value else []
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []

