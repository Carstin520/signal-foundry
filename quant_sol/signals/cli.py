from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import typer
from requests import RequestException
from rich.console import Console
from rich.table import Table

from .accounts import (
    account_rows_from_config,
    export_account_seed_csv,
    import_accounts_csv,
    import_follow_graph_csv,
    import_posts_csv,
    rank_accounts as rank_web3_accounts,
    write_account_report,
)
from .clients import CLOBClient, CLOBWebSocket, DataApiClient, GammaMarketClient, XApiClient
from .config import (
    SIGNAL_REPORT_ROOT,
    load_fomo_config,
    load_market_rules,
    load_social_handles,
    load_wallet_watchlist,
    load_web3_accounts,
    load_web3_keywords,
    load_x_api_limits,
    parse_duration,
)
from .env import has_secret, load_local_env, masked_secret
from .diagnostics import model_diagnostics, write_model_diagnostics
from .history import (
    case_keywords,
    discover_event_case as discover_event_case_record,
    event_case_token_rows,
    get_event_case,
    normalize_price_history,
    run_event_backtest as run_event_backtest_case,
    store_event_case_posts,
    write_event_backtest_report,
    x_case_query,
    x_time,
)
from .reporting import write_signal_report
from .scoring import evaluate_signal_outcomes, score_recent, should_alert
from .storage import (
    active_markets,
    api_calls_today,
    alerted_signal_ids,
    connect,
    insert_market_midpoint_tick,
    insert_historical_price_ticks,
    insert_market_tick,
    record_api_call,
    record_telegram_alert,
    replace_wallet_activity,
    save_raw_payload,
    upsert_markets,
    upsert_signal_events,
    upsert_social_posts,
    upsert_x_accounts,
    upsert_x_follow_graph,
    upsert_x_posts,
)
from .telegram import TelegramClient, format_alert
from .utils import first_float, first_text, parse_timestamp, utc_now_iso


app = typer.Typer(help="Read-only prediction-market information arbitrage Research OS.")
console = Console()
load_local_env()


@app.command("discover-markets")
def discover_markets(
    category: str = typer.Option("politics", "--category", help="Market category focus: politics or crypto."),
    max_pages: int = typer.Option(3, "--max-pages", help="Gamma API pages to scan."),
) -> None:
    """Discover active Polymarket markets through Gamma."""
    keywords = _market_keywords_for_category(category)
    client = GammaMarketClient()
    records = client.discover_markets(keywords, max_pages=max_pages, category=category)
    con = connect()
    count = upsert_markets(con, records)
    save_raw_payload("gamma_markets", category, [record.raw for record in records])
    console.print(f"Discovered {count} {category} market(s).")


@app.command("sync-social")
def sync_social(
    backfill: str = typer.Option("24h", "--backfill", help="Backfill window, e.g. 1h, 24h, 7d."),
    csv_path: Optional[str] = typer.Option(None, "--csv", help="Optional CSV fallback with handle,post_id,created_at,text,url columns."),
    validate_handles: bool = typer.Option(False, "--validate-handles", help="Validate configured handles through X API before backfill."),
    max_handles: Optional[int] = typer.Option(None, "--max-handles", help="Limit X handles touched in this run."),
    max_posts_per_handle: Optional[int] = typer.Option(None, "--max-posts-per-handle", help="Limit timeline rows requested per handle."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate calls without contacting X API."),
) -> None:
    """Backfill public X posts from configured watchlist handles."""
    con = connect()
    if csv_path:
        posts = _posts_from_csv(csv_path)
        count = upsert_social_posts(con, posts)
        console.print(f"Imported {count} social post(s) from CSV.")
        return

    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        console.print("[yellow]X_BEARER_TOKEN is not set. Use --csv for manual fallback or configure official X API access.[/yellow]")
        return

    limits = load_x_api_limits()
    handle_limit = max_handles or limits.sync_social_max_handles
    post_limit = max_posts_per_handle or limits.sync_social_max_posts_per_handle
    cap = daily_cap or limits.daily_call_cap
    handles = load_social_handles()[:handle_limit]
    planned_calls = len(handles) * (3 if validate_handles else 2)
    if not _check_x_budget(con, planned_calls, cap, dry_run, f"sync-social handles={len(handles)} max_posts={post_limit}"):
        return
    if dry_run:
        return
    client = XApiClient(token)
    seconds = parse_duration(backfill)
    total = 0
    inactive = []
    for handle in handles:
        try:
            if validate_handles and not client.validate_handle(handle.handle):
                record_api_call(con, "x", "users/by/username", notes=f"validate @{handle.handle}")
                inactive.append(handle.handle)
                continue
            if validate_handles:
                record_api_call(con, "x", "users/by/username", notes=f"validate @{handle.handle}")
            posts = client.backfill_user_posts(handle.handle, seconds, max_results=post_limit)
            record_api_call(con, "x", "users/by/username", notes=f"resolve @{handle.handle}")
            record_api_call(con, "x", "users/:id/tweets", notes=f"timeline @{handle.handle}")
        except RequestException as exc:
            console.print(f"[yellow]warning: @{handle.handle} backfill failed: {exc}[/yellow]")
            continue
        upsert_social_posts(con, posts)
        if posts:
            save_raw_payload("x_posts", handle.handle, [post.raw for post in posts])
        total += len(posts)
    console.print(f"Synced {total} X post(s).")
    if inactive:
        console.print(f"[yellow]Inactive handles: {', '.join(inactive)}[/yellow]")


@app.command("check-api")
def check_api(
    service: str = typer.Option("x", "--service", help="API service to check. V1 supports x."),
    handle: str = typer.Option("WuBlockchain", "--handle", help="X handle used for a minimal read-only test call."),
    no_call: bool = typer.Option(False, "--no-call", help="Only verify local env configuration; do not call external API."),
) -> None:
    """Check local API credentials before running paid data sync commands."""
    load_local_env()
    if service != "x":
        console.print(f"[yellow]Unsupported service '{service}'. V1 supports x.[/yellow]")
        return
    if not has_secret("X_BEARER_TOKEN"):
        console.print("[yellow]X_BEARER_TOKEN is missing. Copy .env.example to .env and paste your Bearer Token.[/yellow]")
        return
    console.print(f"X_BEARER_TOKEN configured: {masked_secret('X_BEARER_TOKEN')}")
    if no_call:
        console.print("Skipped external API call.")
        return
    con = connect()
    if not _check_x_budget(con, 1, load_x_api_limits().daily_call_cap, False, "check-api"):
        return
    try:
        profile = XApiClient(os.environ["X_BEARER_TOKEN"]).user_profile(handle.lstrip("@"))
    except RequestException as exc:
        console.print(f"[red]X API check failed: {exc}[/red]")
        return
    record_api_call(con, "x", "users/by/username", notes=f"check @{handle.lstrip('@')}")
    if not profile:
        console.print(f"[yellow]X API responded, but @{handle.lstrip('@')} was not resolved.[/yellow]")
        return
    metrics = profile.get("public_metrics") or {}
    console.print(
        f"X API OK: @{profile.get('username', handle.lstrip('@'))} "
        f"id={profile.get('id')} followers={metrics.get('followers_count', 'n/a')}"
    )


@app.command("discover-accounts")
def discover_accounts(
    source: str = typer.Option("chrome-notes", "--source", help="Candidate source label. V1 supports chrome-notes/config CSV seeding."),
    csv_path: Optional[str] = typer.Option(None, "--csv", help="Optional account CSV with handle,language,region,role,priority,notes."),
) -> None:
    """Seed Web3 X account candidates discovered through Chrome-assisted research."""
    con = connect()
    if source not in {"chrome-notes", "config"}:
        console.print(f"[yellow]Unknown source '{source}'. Seeding configured Web3 accounts only.[/yellow]")
    configured = load_web3_accounts()
    seeded = upsert_x_accounts(con, account_rows_from_config(configured))
    imported = import_accounts_csv(con, Path(csv_path)) if csv_path else 0
    console.print(f"Seeded {seeded} configured Web3 account(s); imported {imported} CSV account(s).")


@app.command("sync-accounts")
def sync_accounts(
    watchlist: str = typer.Option("web3", "--watchlist", help="Account watchlist group. V1 supports web3."),
    backfill: str = typer.Option("7d", "--backfill", help="Timeline backfill window, e.g. 24h, 7d."),
    accounts_csv: Optional[str] = typer.Option(None, "--accounts-csv", help="Optional CSV fallback for account profile rows."),
    posts_csv: Optional[str] = typer.Option(None, "--posts-csv", help="Optional CSV fallback for post rows."),
    max_accounts: Optional[int] = typer.Option(None, "--max-accounts", help="Limit X accounts touched in this run."),
    max_posts_per_account: Optional[int] = typer.Option(None, "--max-posts-per-account", help="Limit timeline rows requested per account."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate calls without contacting X API."),
) -> None:
    """Sync Web3 X account profiles and posts through X API, or import CSV fallback data."""
    if watchlist != "web3":
        console.print(f"[yellow]No account watchlist named {watchlist}.[/yellow]")
        return

    con = connect()
    accounts = load_web3_accounts()
    seeded = upsert_x_accounts(con, account_rows_from_config(accounts))
    imported_accounts = import_accounts_csv(con, Path(accounts_csv)) if accounts_csv else 0
    imported_posts = import_posts_csv(con, Path(posts_csv)) if posts_csv else 0
    if imported_accounts or imported_posts:
        console.print(f"Imported {imported_accounts} account row(s) and {imported_posts} post row(s) from CSV.")

    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        console.print(
            "[yellow]X_BEARER_TOKEN is not set. Seeded config accounts only; use --posts-csv/--accounts-csv "
            "or configure official X API access for structured backfill.[/yellow]"
        )
        console.print(f"Seeded {seeded} configured Web3 account(s).")
        return

    limits = load_x_api_limits()
    account_limit = max_accounts or limits.sync_accounts_max_accounts
    post_limit = max_posts_per_account or limits.sync_accounts_max_posts_per_account
    cap = daily_cap or limits.daily_call_cap
    selected_accounts = accounts[:account_limit]
    planned_calls = len(selected_accounts) * 3
    if not _check_x_budget(
        con,
        planned_calls,
        cap,
        dry_run,
        f"sync-accounts accounts={len(selected_accounts)} max_posts={post_limit}",
    ):
        return
    if dry_run:
        return

    client = XApiClient(token)
    seconds = parse_duration(backfill)
    profile_count = 0
    post_count = 0
    inactive = []
    for account in selected_accounts:
        try:
            profile = client.user_profile(account.handle)
            record_api_call(con, "x", "users/by/username", notes=f"profile @{account.handle}")
            if not profile:
                inactive.append(account.handle)
                upsert_x_accounts(
                    con,
                    [
                        {
                            **account_rows_from_config([account])[0],
                            "status": "inactive_handle",
                        }
                    ],
                )
                continue
            metrics = profile.get("public_metrics") or {}
            upsert_x_accounts(
                con,
                [
                    {
                        "handle": account.handle,
                        "user_id": profile.get("id"),
                        "language": account.language,
                        "role": account.role,
                        "region": account.region,
                        "priority": account.priority,
                        "followers": metrics.get("followers_count"),
                        "following": metrics.get("following_count"),
                        "verified": profile.get("verified"),
                        "profile_metrics": metrics,
                        "status": "active",
                        "notes": account.notes,
                    }
                ],
            )
            profile_count += 1
            posts = client.backfill_user_post_dicts(account.handle, seconds, max_results=post_limit)
            record_api_call(con, "x", "users/by/username", notes=f"resolve @{account.handle}")
            record_api_call(con, "x", "users/:id/tweets", notes=f"timeline @{account.handle}")
        except RequestException as exc:
            console.print(f"[yellow]warning: @{account.handle} account sync failed: {exc}[/yellow]")
            continue
        if profile:
            save_raw_payload("x_account_profiles", account.handle, profile)
        if posts:
            save_raw_payload("x_account_posts", account.handle, [post.get("raw_json") or post for post in posts])
            post_count += upsert_x_posts(con, posts)

    console.print(f"Synced {profile_count} X account profile(s) and {post_count} post(s).")
    if inactive:
        console.print(f"[yellow]Inactive handles: {', '.join(inactive)}[/yellow]")


@app.command("sync-follow-graph")
def sync_follow_graph(
    top: Optional[int] = typer.Option(None, "--top", help="Number of ranked/configured accounts to inspect."),
    max_following_per_account: Optional[int] = typer.Option(None, "--max-following-per-account", help="Limit following rows requested per account."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    csv_path: Optional[str] = typer.Option(None, "--csv", help="Optional CSV fallback with source_handle,target_handle,relationship."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate calls without contacting X API."),
) -> None:
    """Sync following edges for high-signal accounts, or import follow-graph CSV fallback data."""
    con = connect()
    if csv_path:
        imported = import_follow_graph_csv(con, Path(csv_path))
        console.print(f"Imported {imported} follow graph edge(s) from CSV.")
        return

    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        console.print("[yellow]X_BEARER_TOKEN is not set. Use --csv to import follow graph notes from Chrome review.[/yellow]")
        return

    limits = load_x_api_limits()
    account_limit = top or limits.sync_follow_graph_max_accounts
    following_limit = max_following_per_account or limits.sync_follow_graph_max_following_per_account
    handles = _top_account_handles(con, account_limit)
    if not handles:
        handles = [account.handle for account in load_web3_accounts()[:account_limit]]
    handles = handles[:account_limit]
    planned_calls = len(handles) * 2
    if not _check_x_budget(
        con,
        planned_calls,
        daily_cap or limits.daily_call_cap,
        dry_run,
        f"sync-follow-graph accounts={len(handles)} max_following={following_limit}",
    ):
        return
    if dry_run:
        return

    client = XApiClient(token)
    edge_count = 0
    upstream_profiles = []
    for handle in handles:
        try:
            following = client.following(handle, max_results=following_limit)
            record_api_call(con, "x", "users/by/username", notes=f"resolve @{handle}")
            record_api_call(con, "x", "users/:id/following", notes=f"following @{handle}")
        except RequestException as exc:
            console.print(f"[yellow]warning: @{handle} following sync failed: {exc}[/yellow]")
            continue
        edges = []
        for item in following:
            target = item.get("username")
            if not target:
                continue
            metrics = item.get("public_metrics") or {}
            edges.append({"source_handle": handle, "target_handle": target, "relationship": "following"})
            upstream_profiles.append(
                {
                    "handle": target,
                    "user_id": item.get("id"),
                    "language": "mixed",
                    "role": "upstream_sources",
                    "region": "global",
                    "priority": "watch",
                    "followers": metrics.get("followers_count"),
                    "following": metrics.get("following_count"),
                    "verified": item.get("verified"),
                    "profile_metrics": metrics,
                    "status": "active",
                    "notes": f"Discovered from @{handle} following graph.",
                }
            )
        edge_count += upsert_x_follow_graph(con, edges)
        if following:
            save_raw_payload("x_following", handle, following)
    if upstream_profiles:
        upsert_x_accounts(con, upstream_profiles)
    console.print(f"Synced {edge_count} follow graph edge(s) from {len(handles)} account(s).")


@app.command("rank-accounts")
def rank_accounts(
    lookback: str = typer.Option("30d", "--lookback", help="Ranking lookback window, e.g. 7d, 30d."),
) -> None:
    """Rank Web3 X accounts by narrative speed, propagation, source chain, and market impact."""
    metrics = rank_web3_accounts(connect(), lookback)
    _render_account_metrics(metrics)
    console.print(f"Ranked {len(metrics)} Web3 account(s).")


@app.command("report-accounts")
def report_accounts(
    lookback: str = typer.Option("30d", "--lookback", help="Ranking lookback window, e.g. 7d, 30d."),
) -> None:
    """Write a local markdown report for Web3 X account ranking."""
    path = write_account_report(connect(), lookback, SIGNAL_REPORT_ROOT)
    console.print(f"Wrote account report: {path}")


@app.command("diagnose-model")
def diagnose_model(
    date: Optional[str] = typer.Option(None, "--date", help="Optional report suffix date YYYY-MM-DD."),
) -> None:
    """Write a local diagnostics report for model repair completion and data coverage."""
    con = connect()
    diagnostics = model_diagnostics(con)
    path = write_model_diagnostics(con, SIGNAL_REPORT_ROOT, date=date)
    blockers = diagnostics["blockers"]
    console.print(f"Wrote model diagnostics: {path}")
    if blockers:
        console.print("[yellow]Open blockers: " + ", ".join(blockers) + "[/yellow]")
    else:
        console.print("No open model blockers detected.")


@app.command("discover-event-case")
def discover_event_case(
    query: str = typer.Option(..., "--query", help="Event query, e.g. 'Trump China visit'."),
    start: str = typer.Option(..., "--start", help="Case start date/time, e.g. YYYY-MM-DD."),
    end: str = typer.Option(..., "--end", help="Case end date/time, e.g. YYYY-MM-DD."),
    case: Optional[str] = typer.Option(None, "--case", help="Optional case id. Defaults to slugified query."),
    market_slug: Optional[str] = typer.Option(None, "--market-slug", help="Optional exact market slug override."),
    max_pages: int = typer.Option(4, "--max-pages", help="Gamma API pages to scan, open and closed."),
) -> None:
    """Discover and register a historical event backtest case."""
    con = connect()
    result = discover_event_case_record(
        con,
        query=query,
        start_at=parse_timestamp(start) or start,
        end_at=parse_timestamp(end) or end,
        case_id=case,
        max_pages=max_pages,
        market_slug=market_slug,
    )
    selected = result.get("selected_market")
    if selected:
        save_raw_payload("event_cases", result["case_id"], selected.raw)
        console.print(
            f"Registered case {result['case_id']} -> {selected.market_slug} "
            f"({result['candidate_count']} candidate market(s))."
        )
    else:
        console.print(f"[yellow]Registered unresolved case {result['case_id']}; no matching market found.[/yellow]")


@app.command("backfill-market-history")
def backfill_market_history(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    interval: str = typer.Option("1m", "--interval", help="Polymarket history interval: 1m, 1h, 6h, 1d, 1w, all, max."),
    fidelity: int = typer.Option(1, "--fidelity", help="History fidelity in minutes."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show request plan without calling CLOB."),
) -> None:
    """Backfill Polymarket historical prices for an event case."""
    con = connect()
    case_row = get_event_case(con, case)
    if not case_row:
        console.print(f"[yellow]No event case named {case}. Run discover-event-case first.[/yellow]")
        return
    token_rows = event_case_token_rows(con, case)
    start_dt = _to_datetime_or_none(case_row.get("start_at"))
    end_dt = _to_datetime_or_none(case_row.get("end_at"))
    if not token_rows or start_dt is None or end_dt is None:
        console.print("[yellow]Case is missing market token ids or valid time window.[/yellow]")
        return
    planned_calls = len(_chunks(token_rows, 20))
    console.print(
        f"Polymarket history plan: case={case}, market={case_row['market_slug']}, "
        f"tokens={len(token_rows)}, requests={planned_calls}, interval={interval}, fidelity={fidelity}"
    )
    if dry_run:
        console.print("Dry run only: no CLOB calls made.")
        return
    client = CLOBClient()
    total = 0
    for chunk in _chunks(token_rows, 20):
        token_ids = [row["token_id"] for row in chunk]
        payload = client.batch_prices_history(
            token_ids,
            start_ts=int(start_dt.timestamp()),
            end_ts=int(end_dt.timestamp()),
            interval=interval,
            fidelity=fidelity,
        )
        save_raw_payload("polymarket_price_history", case, payload)
        token_to_market = {row["token_id"]: row for row in chunk}
        rows = normalize_price_history(payload, token_to_market)
        total += insert_historical_price_ticks(con, rows)
    console.print(f"Backfilled {total} historical price tick(s).")


@app.command("backfill-x-history")
def backfill_x_history(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    mode: str = typer.Option("x-api", "--mode", help="History source. V1 supports x-api."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    max_accounts: Optional[int] = typer.Option(None, "--max-accounts", help="Maximum signal accounts to query."),
    max_posts_per_account: int = typer.Option(25, "--max-posts-per-account", help="Maximum posts requested per account."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate X calls without contacting X API."),
) -> None:
    """Backfill X posts for an event case using official X full-archive API."""
    if mode != "x-api":
        console.print(f"[yellow]Unsupported mode '{mode}'. V1 supports x-api.[/yellow]")
        return
    con = connect()
    case_row = get_event_case(con, case)
    if not case_row:
        console.print(f"[yellow]No event case named {case}. Run discover-event-case first.[/yellow]")
        return
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        console.print("[yellow]X_BEARER_TOKEN is not set. Cannot use X full-archive search.[/yellow]")
        return
    accounts = load_web3_accounts()
    selected = accounts[: max_accounts or load_x_api_limits().sync_accounts_max_accounts]
    planned_calls = len(selected) * 2
    cap = daily_cap or load_x_api_limits().daily_call_cap
    if not _check_x_budget(con, planned_calls, cap, dry_run, f"backfill-x-history case={case} accounts={len(selected)}"):
        return
    if dry_run:
        return

    client = XApiClient(token)
    keywords = case_row.get("keywords") or case_keywords(str(case_row.get("query") or ""))
    start_time = x_time(case_row["start_at"])
    end_time = x_time(case_row["end_at"])
    total = 0
    for account in selected:
        query = x_case_query(account.handle, keywords)
        try:
            counts = client.full_archive_counts(query, start_time, end_time)
            record_api_call(con, "x", "tweets/counts/all", notes=f"case={case} @{account.handle}")
            posts = client.full_archive_search(query, start_time, end_time, max_results=max_posts_per_account)
            record_api_call(con, "x", "tweets/search/all", notes=f"case={case} @{account.handle}")
        except RequestException as exc:
            if _http_status(exc) in {401, 403}:
                console.print(
                    "[yellow]X full-archive search is unavailable for this token. "
                    "Historical backtest cannot be populated from X API; use a recent 7-day case or upgrade X access.[/yellow]"
                )
                return
            console.print(f"[yellow]warning: X history failed for @{account.handle}: {exc}[/yellow]")
            continue
        save_raw_payload("x_event_history", f"{case}_{account.handle}_counts", counts)
        if posts:
            save_raw_payload("x_event_history", f"{case}_{account.handle}_posts", [post.get("raw_json") or post for post in posts])
        total += store_event_case_posts(con, case, posts, keywords)
    console.print(f"Backfilled {total} event case post(s).")


@app.command("run-event-backtest")
def run_event_backtest(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    horizons: str = typer.Option("1h,6h,24h,72h", "--horizons", help="Comma-separated horizons."),
) -> None:
    """Run post-level event-study backtest for an event case."""
    horizon_values = [part.strip() for part in horizons.split(",") if part.strip()]
    impacts, metrics = run_event_backtest_case(connect(), case, horizon_values)
    console.print(f"Backtested {len(impacts)} post impact row(s); wrote {len(metrics)} account metric row(s).")


@app.command("report-event-backtest")
def report_event_backtest(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
) -> None:
    """Write a local markdown historical event backtest report."""
    path = write_event_backtest_report(connect(), case, SIGNAL_REPORT_ROOT)
    console.print(f"Wrote event backtest report: {path}")


@app.command("export-account-seeds")
def export_account_seeds(
    format: str = typer.Option("csv", "--format", help="Export format. V1 supports csv."),
    output: Optional[str] = typer.Option(None, "--output", help="Output path. Defaults to data/reports/web3_account_seeds.csv."),
) -> None:
    """Export configured Web3 seed accounts for manual editing or CSV workflow."""
    if format != "csv":
        console.print(f"[yellow]Unsupported format '{format}'. V1 supports csv.[/yellow]")
        return
    path = Path(output) if output else SIGNAL_REPORT_ROOT / "web3_account_seeds.csv"
    written = export_account_seed_csv(load_web3_accounts(), path)
    console.print(f"Exported account seeds: {written}")


@app.command("sync-wallets")
def sync_wallets(
    watchlist: str = typer.Option("political", "--watchlist", help="Wallet watchlist group."),
    max_rows: int = typer.Option(5000, "--max-rows", help="Maximum activity rows per wallet."),
) -> None:
    """Sync public wallet activity for signal context."""
    wallets = load_wallet_watchlist(watchlist)
    if not wallets:
        console.print(f"[yellow]No wallet watchlist named {watchlist}.[/yellow]")
        return
    con = connect()
    client = DataApiClient()
    total = 0
    for label, address in wallets.items():
        try:
            rows = client.activity(address, max_rows=max_rows)
        except RequestException as exc:
            console.print(f"[yellow]warning: {label} activity fetch failed: {exc}[/yellow]")
            continue
        save_raw_payload("wallet_activity", label, rows)
        total += replace_wallet_activity(con, label, rows)
    console.print(f"Synced {total} wallet activity row(s).")


@app.command("stream-market")
def stream_market(
    watchlist: str = typer.Option("political", "--watchlist", help="Market watchlist group. V1 streams all discovered markets."),
    seconds: int = typer.Option(60, "--seconds", help="How long to stream CLOB market updates."),
) -> None:
    """Subscribe to public CLOB market WebSocket for discovered markets."""
    con = connect()
    markets = active_markets(con)
    token_to_market = {}
    for market in markets:
        for token_id in _json_list(market.get("clob_token_ids")):
            token_to_market[token_id] = market["market_slug"]
    if not token_to_market:
        console.print("[yellow]No token ids found. Run discover-markets first.[/yellow]")
        return

    def on_message(payload: dict) -> None:
        token_id = first_text(payload, "asset_id", "assetId", "token_id", "tokenId")
        best_bid = first_float(payload, "best_bid", "bid", "bestBid")
        best_ask = first_float(payload, "best_ask", "ask", "bestAsk")
        last_trade = first_float(payload, "price", "last_trade_price", "lastTradePrice")
        market_slug = token_to_market.get(str(token_id))
        insert_market_tick(con, parse_timestamp(payload.get("timestamp")) or utc_now_iso(), market_slug, token_id, best_bid, best_ask, last_trade, None, payload)

    CLOBWebSocket().stream(list(token_to_market), on_message=on_message, seconds=seconds)
    console.print(f"Streamed CLOB updates for {seconds}s from {len(token_to_market)} token(s).")


@app.command("sync-market-ticks")
def sync_market_ticks(
    category: str = typer.Option("crypto", "--category", help="Market category filter: crypto, politics, or all."),
    max_markets: int = typer.Option(20, "--max-markets", help="Maximum markets to snapshot."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate token snapshots without calling CLOB."),
) -> None:
    """Snapshot public CLOB midpoint prices for discovered markets."""
    con = connect()
    markets, token_rows = _market_tick_plan(con, category, max_markets)
    console.print(f"Polymarket CLOB plan: markets={len(markets)}, token_midpoints={len(token_rows)}")
    if dry_run:
        console.print("Dry run only: no CLOB calls made.")
        return
    count = _snapshot_market_ticks(con, token_rows)
    console.print(f"Synced {count} CLOB midpoint tick(s).")


@app.command("collect-market-ticks")
def collect_market_ticks(
    category: str = typer.Option("all", "--category", help="Market category filter: crypto, politics, or all."),
    max_markets: int = typer.Option(20, "--max-markets", help="Maximum markets to snapshot per iteration."),
    interval_seconds: int = typer.Option(300, "--interval-seconds", help="Seconds between midpoint snapshots."),
    iterations: int = typer.Option(12, "--iterations", help="Number of snapshot iterations. Use a bounded value."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show collection plan without calling CLOB."),
) -> None:
    """Continuously collect bounded Polymarket CLOB midpoint snapshots."""
    con = connect()
    markets, token_rows = _market_tick_plan(con, category, max_markets)
    planned_calls = len(token_rows) * max(0, iterations)
    console.print(
        f"Polymarket CLOB continuous plan: category={category}, markets={len(markets)}, "
        f"token_midpoints_per_iteration={len(token_rows)}, iterations={iterations}, "
        f"interval_seconds={interval_seconds}, planned_calls={planned_calls}"
    )
    if iterations <= 0:
        console.print("[yellow]iterations must be positive.[/yellow]")
        return
    if interval_seconds < 0:
        console.print("[yellow]interval-seconds must be non-negative.[/yellow]")
        return
    if not token_rows:
        console.print("[yellow]No token ids found. Run discover-markets first.[/yellow]")
        return
    if dry_run:
        console.print("Dry run only: no CLOB calls made.")
        return

    total = 0
    for index in range(iterations):
        count = _snapshot_market_ticks(con, token_rows)
        total += count
        console.print(f"Iteration {index + 1}/{iterations}: synced {count} midpoint tick(s).")
        if index < iterations - 1:
            time.sleep(interval_seconds)
    console.print(f"Collected {total} CLOB midpoint tick(s) across {iterations} iteration(s).")


@app.command("score")
def score(
    since: str = typer.Option("24h", "--since", help="Score narrative windows since this duration."),
) -> None:
    """Generate market-level FOMO divergence scores from local market/social/wallet data."""
    con = connect()
    since_iso = _since_iso(since)
    signals = score_recent(con, since_iso, load_social_handles(), load_market_rules(), load_fomo_config())
    upsert_signal_events(con, signals)
    _render_signals(signals)
    console.print(f"Generated {len(signals)} signal(s).")


@app.command("alert")
def alert(
    since: str = typer.Option("1h", "--since", help="Alert on FOMO signals generated since this window."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Render and record without sending Telegram."),
) -> None:
    """Send Telegram alerts for high-confidence FOMO divergence signals."""
    con = connect()
    rows = _recent_signal_rows(con, _since_iso(since))
    sent = alerted_signal_ids(con)
    client = TelegramClient()
    count = 0
    for row in rows:
        signal = _signal_from_row(row)
        if signal.signal_id in sent or not should_alert(signal, threshold=load_fomo_config().alert_threshold):
            continue
        payload = format_alert(signal)
        status, error = client.send_message(payload, dry_run=dry_run)
        record_telegram_alert(con, signal.signal_id, payload, "sent" if status == "sent" else status, error)
        count += 1
        if status != "sent":
            console.print(f"[yellow]{signal.signal_id}: {status} {error or ''}[/yellow]")
    console.print(f"Processed {count} alert candidate(s).")


@app.command("evaluate")
def evaluate(
    horizon: str = typer.Option("24h", "--horizon", help="Outcome horizon, e.g. 6h, 24h, 72h."),
) -> None:
    """Evaluate whether prior FOMO signals saw later price convergence."""
    outcomes = evaluate_signal_outcomes(connect(), horizon, load_fomo_config())
    console.print(f"Evaluated {len(outcomes)} signal outcome(s) for horizon {horizon}.")


@app.command("report")
def report(
    date: str = typer.Option(datetime.now(timezone.utc).date().isoformat(), "--date", help="Report date YYYY-MM-DD."),
) -> None:
    """Write a local markdown signal report."""
    path = write_signal_report(connect(), date)
    console.print(f"Wrote report: {path}")


def _top_account_handles(con, limit: int) -> list[str]:
    metric_rows = con.execute(
        """
        select account
        from account_impact_metrics
        order by final_score desc
        limit ?
        """,
        [limit],
    ).fetchall()
    if metric_rows:
        return [str(row[0]) for row in metric_rows]
    account_rows = con.execute(
        """
        select handle
        from x_accounts
        where coalesce(status, 'active') = 'active'
        order by coalesce(followers, 0) desc, handle
        limit ?
        """,
        [limit],
    ).fetchall()
    return [str(row[0]) for row in account_rows]


def _market_keywords_for_category(category: str) -> list[str]:
    if category in {"crypto", "web3"}:
        keywords = load_web3_keywords()
        terms = []
        for group_terms in keywords.groups.values():
            terms.extend(group_terms)
        terms.extend(["crypto", "bitcoin", "ethereum", "solana", "binance", "coinbase", "hyperliquid"])
        return list(dict.fromkeys(str(term).lower() for term in terms if term))
    return list(load_market_rules().keywords)


def _markets_for_tick_sync(con, category: str, max_markets: int) -> list[dict]:
    markets = active_markets(con)
    if category == "all":
        return markets[:max_markets]
    keywords = _market_keywords_for_category(category)
    matched = []
    for market in markets:
        haystack = " ".join(
            [
                str(market.get("market_slug") or ""),
                str(market.get("event_slug") or ""),
                str(market.get("question") or ""),
                str(market.get("category") or ""),
                " ".join(_json_list(market.get("tags"))),
            ]
        ).lower()
        if any(keyword in haystack for keyword in keywords):
            matched.append(market)
    return matched[:max_markets]


def _market_tick_plan(con, category: str, max_markets: int):
    markets = _markets_for_tick_sync(con, category, max_markets)
    token_rows = []
    for market in markets:
        for token_id in _json_list(market.get("clob_token_ids"))[:1]:
            token_rows.append((market["market_slug"], token_id, market.get("liquidity")))
    return markets, token_rows


def _snapshot_market_ticks(con, token_rows) -> int:
    client = CLOBClient()
    observed_at = utc_now_iso()
    count = 0
    for market_slug, token_id, liquidity in token_rows:
        try:
            mid = client.midpoint(token_id)
        except RequestException as exc:
            console.print(f"[yellow]warning: midpoint failed for {market_slug}/{token_id}: {exc}[/yellow]")
            continue
        insert_market_midpoint_tick(con, observed_at, market_slug, token_id, mid, liquidity, {"mid": mid, "token_id": token_id})
        count += 1
    return count


def _chunks(rows, size: int):
    return [rows[index:index + size] for index in range(0, len(rows), size)]


def _to_datetime_or_none(value):
    from .utils import to_datetime

    return to_datetime(value)


def _http_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return int(status) if status is not None else None


def _check_x_budget(con, planned_calls: int, daily_cap: int, dry_run: bool, label: str) -> bool:
    used = api_calls_today(con, "x")
    remaining = max(0, daily_cap - used)
    console.print(
        f"X API budget: used_today={used}, planned_calls={planned_calls}, "
        f"daily_cap={daily_cap}, remaining_after={remaining - planned_calls}"
    )
    if planned_calls > remaining:
        console.print(
            f"[yellow]Stopped before external calls: {label} would exceed the local daily cap. "
            "Increase --daily-cap explicitly if you really want to run it.[/yellow]"
        )
        return False
    if dry_run:
        console.print(f"Dry run only: {label}. No X API calls made.")
    return True


def _render_account_metrics(metrics: list[dict]) -> None:
    table = Table(title="Web3 X Account Influence Ranking")
    table.add_column("Rank", justify="right")
    table.add_column("Account")
    table.add_column("Final", justify="right")
    table.add_column("Speed", justify="right")
    table.add_column("Freq", justify="right")
    table.add_column("Cascade", justify="right")
    table.add_column("Market", justify="right")
    table.add_column("Chain", justify="right")
    table.add_column("False FOMO", justify="right")
    table.add_column("Status")
    for idx, row in enumerate(metrics[:30], start=1):
        table.add_row(
            str(idx),
            f"@{row['account']}",
            _fmt(row.get("final_score")),
            _fmt(row.get("speed_score")),
            _fmt(row.get("frequency_score")),
            _fmt(row.get("cascade_score")),
            _fmt(row.get("market_impact_score")),
            _fmt(row.get("source_chain_score")),
            _fmt(row.get("false_fomo_rate")),
            str(row.get("recommended_status") or ""),
        )
    console.print(table)


def _since_iso(duration: str) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=parse_duration(duration))).replace(microsecond=0).isoformat()


def _render_signals(signals) -> None:
    table = Table(title="FOMO Divergence Scores")
    table.add_column("Score", justify="right")
    table.add_column("Market")
    table.add_column("Narrative")
    table.add_column("Mid", justify="right")
    table.add_column("6h move", justify="right")
    table.add_column("Capacity", justify="right")
    table.add_column("Confidence")
    table.add_column("Risk tags")
    for signal in sorted(signals, key=lambda item: item.score, reverse=True)[:20]:
        price = signal.price_window
        table.add_row(
            str(signal.score),
            signal.market_slug,
            str(price.get("narrative_direction")),
            _fmt(price.get("current_market_probability")),
            _fmt(price.get("market_move_6h")),
            _fmt(price.get("fomo_capacity")),
            signal.confidence,
            ", ".join(signal.risk_tags),
        )
    console.print(table)


def _posts_from_csv(path: str):
    import csv

    from .models import SocialPost

    posts = []
    with open(path, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            post_id = row.get("post_id") or str(abs(hash((row.get("handle"), row.get("created_at"), row.get("text")))))
            posts.append(
                SocialPost(
                    platform="x",
                    handle=(row.get("handle") or "").lstrip("@"),
                    post_id=post_id,
                    created_at=parse_timestamp(row.get("created_at")) or utc_now_iso(),
                    text=row.get("text") or "",
                    url=row.get("url") or "",
                    raw=dict(row),
                )
            )
    return posts


def _json_list(value: object):
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


def _recent_signal_rows(con, since_iso: str):
    rows = con.execute(
        """
        select signal_id, event_family, market_slug, direction_hint, score, confidence,
               evidence, risk_tags, source_posts, wallet_flows, price_window
        from signal_events
        where generated_at >= ?
        order by score desc
        """,
        [since_iso],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _signal_from_row(row: dict):
    from .models import SignalScore

    return SignalScore(
        signal_id=row["signal_id"],
        event_family=row["event_family"],
        market_slug=row["market_slug"],
        direction_hint=row["direction_hint"],
        score=int(row["score"]),
        confidence=row["confidence"],
        evidence=_loads(row["evidence"], {}),
        risk_tags=_loads(row["risk_tags"], []),
        source_posts=_loads(row["source_posts"], []),
        wallet_flows=_loads(row["wallet_flows"], []),
        price_window=_loads(row["price_window"], {}),
    )


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
