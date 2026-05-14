from typing import List, Optional

import typer
from requests import RequestException
from rich.console import Console

from .analysis import analyze_wallets
from .client import GammaClient, PolymarketClient, WALLET_ENDPOINTS
from .config import WATCHLIST
from .reporting import render_console_report, write_markdown_report
from .storage import (
    apply_market_metadata,
    connect,
    latest_metrics,
    missing_event_slugs,
    save_payload,
    seed_wallets,
    upsert_market_metadata,
    utc_now_iso,
)


app = typer.Typer(help="Read-only Polymarket wallet collector and analyzer.")
console = Console()


def _resolve_labels(all_wallets: bool, wallet: Optional[str]) -> List[str]:
    if all_wallets:
        return list(WATCHLIST.keys())
    if wallet is None:
        raise typer.BadParameter("Pass --all or --wallet <label>.")
    if wallet not in WATCHLIST:
        raise typer.BadParameter(f"Unknown wallet '{wallet}'. Known labels: {', '.join(WATCHLIST)}")
    return [wallet]


@app.command()
def fetch(
    all_wallets: bool = typer.Option(False, "--all", help="Fetch all configured wallets."),
    wallet: Optional[str] = typer.Option(None, "--wallet", help="Fetch one wallet by label."),
) -> None:
    """Fetch public Polymarket wallet data and store it locally."""
    labels = _resolve_labels(all_wallets, wallet)
    con = connect()
    seed_wallets(con)
    client = PolymarketClient()
    gamma = GammaClient()
    fetched_at = utc_now_iso()

    for label in labels:
        wallet_config = WATCHLIST[label]
        if not wallet_config.is_resolved:
            console.print(f"[yellow]Skipping {label}: {wallet_config.status}[/yellow]")
            continue
        console.print(f"Fetching {label} ({wallet_config.address})")
        for endpoint in WALLET_ENDPOINTS:
            try:
                endpoint_payload = client.get_endpoint(endpoint, wallet_config)
            except RequestException as exc:
                console.print(f"[yellow]  warning: {endpoint} fetch failed: {exc}[/yellow]")
                continue
            path = save_payload(con, wallet_config, endpoint, endpoint_payload.payload, fetched_at=fetched_at)
            console.print(f"  saved {endpoint}: {path}")
        _enrich_wallet_markets(con, gamma, label, fetched_at)


@app.command()
def analyze(
    all_wallets: bool = typer.Option(False, "--all", help="Analyze all configured wallets."),
    wallet: Optional[str] = typer.Option(None, "--wallet", help="Analyze one wallet by label."),
) -> None:
    """Recompute wallet metrics from the local DuckDB database."""
    labels = _resolve_labels(all_wallets, wallet)
    con = connect()
    seed_wallets(con)
    metrics = analyze_wallets(con, labels)
    console.print(f"Analyzed {len(metrics)} wallet(s).")


@app.command()
def report(
    all_wallets: bool = typer.Option(False, "--all", help="Report latest metrics for all wallets."),
    wallet: Optional[str] = typer.Option(None, "--wallet", help="Report one wallet by label."),
) -> None:
    """Render latest wallet metrics and write a local markdown report."""
    labels = _resolve_labels(all_wallets, wallet)
    con = connect()
    seed_wallets(con)
    rows = [row for row in latest_metrics(con) if row["wallet"] in labels]
    if not rows:
        console.print("[yellow]No metrics found. Run analyze first.[/yellow]")
        return
    render_console_report(rows, console=console)
    report_path = write_markdown_report(rows)
    console.print(f"Wrote report: {report_path}")


def _enrich_wallet_markets(con, gamma: GammaClient, wallet_label: str, fetched_at: str) -> None:  # type: ignore[no-untyped-def]
    slugs = missing_event_slugs(con, wallet_label)
    if not slugs:
        return
    console.print(f"  enriching {len(slugs)} market slug(s) from Gamma")
    for slug in slugs:
        try:
            payload = gamma.get_event_by_slug(slug)
        except RequestException as exc:
            console.print(f"[yellow]  warning: Gamma enrichment failed for {slug}: {exc}[/yellow]")
            continue
        upsert_market_metadata(con, slug, payload, fetched_at=fetched_at)
    apply_market_metadata(con)
