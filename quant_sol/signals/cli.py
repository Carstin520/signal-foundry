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
from rich.markup import escape
from rich.table import Table

from .accounts import (
    account_rows_from_config,
    evaluate_account_source,
    export_account_seed_csv,
    import_accounts_csv,
    import_follow_graph_csv,
    import_posts_csv,
    rank_accounts as rank_web3_accounts,
    write_account_report,
    write_account_source_evaluation_report,
)
from .clients import CLOBClient, CLOBWebSocket, DataApiClient, GammaMarketClient, XApiClient
from .config import (
    SIGNAL_REPORT_ROOT,
    load_fomo_config,
    load_market_rules,
    load_semantic_matching_config,
    load_social_handles,
    load_wallet_watchlist,
    load_web3_accounts,
    load_web3_keywords,
    load_x_api_limits,
    parse_duration,
)
from .env import has_secret, load_local_env, masked_secret
from .diagnostics import model_diagnostics, write_model_diagnostics
from .discovery import (
    discover_hyperliquid_hip4_targets,
    discover_kalshi_targets,
    discover_signal_source_candidates,
    planned_x_calls_for_discovery,
    write_latest_hyperliquid_hip4_targets,
    write_latest_kalshi_targets,
    write_latest_polymarket_targets,
    write_signal_discovery_report,
)
from .history import (
    DEFAULT_HORIZONS,
    DEFAULT_MICRO_HORIZONS,
    case_keywords,
    discover_event_case as discover_event_case_record,
    event_price_windows,
    event_case_token_rows,
    get_event_case,
    normalize_price_history,
    run_event_backtest as run_event_backtest_case,
    store_event_case_posts,
    write_event_backtest_report,
    x_case_query,
    x_time,
)
from .price_first import (
    DEFAULT_PRICE_FIRST_HORIZONS,
    DEFAULT_PRICE_WINDOWS,
    match_price_events as match_price_events_case,
    mine_price_events as mine_price_events_case,
    plan_source_backfill as plan_source_backfill_case,
    run_price_first_backtest as run_price_first_backtest_case,
    write_price_first_report,
)
from .reporting import write_signal_report
from .scoring import evaluate_signal_outcomes, score_recent, should_alert
from .storage import (
    active_markets,
    api_calls_today,
    alerted_signal_ids,
    connect,
    insert_market_midpoint_tick,
    insert_market_midpoint_tick_with_source,
    insert_historical_price_ticks,
    insert_market_tick,
    live_burst_run_exists,
    live_burst_trigger_candidates,
    price_events_for_case,
    record_api_call,
    record_telegram_alert,
    replace_wallet_activity,
    save_raw_payload,
    upsert_markets,
    upsert_live_burst_run,
    upsert_signal_events,
    upsert_social_posts,
    upsert_x_accounts,
    upsert_x_follow_graph,
    upsert_x_posts,
)
from .semantic import match_event_posts_semantically, match_event_posts_with_cloud_model
from .telegram import TelegramClient, format_alert
from .utils import first_float, first_text, parse_timestamp, stable_hash, utc_now_iso


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


@app.command("discover-signal-sources")
def discover_signal_sources(
    max_markets: int = typer.Option(8, "--max-markets", help="Maximum high-interest live markets to scan."),
    max_gamma_pages: int = typer.Option(2, "--max-gamma-pages", help="Gamma API pages to scan."),
    min_liquidity: float = typer.Option(100_000, "--min-liquidity", help="Minimum liquidity or volume threshold."),
    focus: str = typer.Option("narrative", "--focus", help="Market focus: narrative, politics, crypto, sports, or all."),
    lookback: str = typer.Option("24h", "--lookback", help="X/reddit discovery lookback window."),
    max_posts_per_market: int = typer.Option(20, "--max-posts-per-market", help="Maximum X posts requested per market."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    include_reddit: bool = typer.Option(True, "--include-reddit/--no-reddit", help="Use public Reddit search as low-confidence context."),
    reddit_limit: int = typer.Option(5, "--reddit-limit", help="Maximum Reddit context posts per market."),
    include_public_seeds: bool = typer.Option(True, "--include-public-seeds/--no-public-seeds", help="Map public preflight X/Reddit/Discord seed sources before X API search."),
    include_kalshi: bool = typer.Option(True, "--include-kalshi/--no-kalshi", help="Include Kalshi public hot markets as read-only cross-venue context."),
    kalshi_limit: int = typer.Option(10, "--kalshi-limit", help="Maximum Kalshi hot markets to include in discovery context."),
    kalshi_max_pages: int = typer.Option(2, "--kalshi-max-pages", help="Kalshi market-list pages to scan for hot-pool context."),
    source_seed_path: Optional[Path] = typer.Option(None, "--source-seed-path", help="Optional public seed config path for discovery V2."),
    latest_targets_path: Optional[Path] = typer.Option(None, "--latest-targets-path", help="Overwrite a concise latest-targets markdown file."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Plan market and X API usage without external X/reddit searches."),
) -> None:
    """Find high-interest open Polymarket pools and candidate X accounts to add as signal sources."""
    con = connect()
    limits = load_x_api_limits()
    cap = daily_cap or limits.daily_call_cap
    planned_x_calls = planned_x_calls_for_discovery(max_markets)
    token = os.getenv("X_BEARER_TOKEN")
    if token and not _check_x_budget(con, planned_x_calls, cap, dry_run, f"discover-signal-sources markets={max_markets}"):
        return
    if not token:
        console.print("[yellow]X_BEARER_TOKEN is not set. Market discovery will run without X source search.[/yellow]")
    result = discover_signal_source_candidates(
        con,
        XApiClient(token) if token and not dry_run else None,
        max_markets=max_markets,
        max_gamma_pages=max_gamma_pages,
        min_liquidity=min_liquidity,
        focus=focus,
        lookback_seconds=parse_duration(lookback),
        max_posts_per_market=max_posts_per_market,
        include_reddit=include_reddit and not dry_run,
        reddit_limit=reddit_limit,
        include_public_seeds=include_public_seeds,
        include_kalshi=include_kalshi,
        kalshi_limit=kalshi_limit,
        kalshi_max_pages=kalshi_max_pages,
        source_seed_path=source_seed_path,
        dry_run=dry_run,
    )
    _render_signal_discovery(result)
    if dry_run:
        console.print("Dry run only: no X/reddit discovery report written.")
        return
    if token:
        record_api_call(con, "x", "tweets/search/recent", call_count=int(result.get("planned_x_calls") or 0), notes="discover-signal-sources")
    path = write_signal_discovery_report(result, SIGNAL_REPORT_ROOT)
    console.print(f"Wrote signal discovery report: {path}")
    if latest_targets_path:
        latest_path = write_latest_polymarket_targets(result, latest_targets_path)
        console.print(f"Wrote latest Polymarket targets: {latest_path}")


@app.command("discover-kalshi-targets")
def discover_kalshi_market_targets(
    max_markets: int = typer.Option(12, "--max-markets", help="Maximum high-interest open Kalshi markets to keep."),
    max_pages: int = typer.Option(2, "--max-pages", help="Kalshi market-list pages to scan."),
    min_volume: float = typer.Option(100_000, "--min-volume", help="Minimum volume or open-interest threshold."),
    focus: str = typer.Option("narrative", "--focus", help="Market focus: narrative, politics, crypto, sports, or all."),
    latest_targets_path: Optional[Path] = typer.Option(None, "--latest-targets-path", help="Overwrite a concise Kalshi latest-targets markdown file."),
) -> None:
    """Find high-interest open Kalshi markets through public market data endpoints."""
    result = discover_kalshi_targets(
        max_markets=max_markets,
        max_pages=max_pages,
        min_volume=min_volume,
        focus=focus,
    )
    _render_kalshi_targets(result)
    if latest_targets_path:
        path = write_latest_kalshi_targets(result, latest_targets_path)
        console.print(f"Wrote latest Kalshi targets: {path}")


@app.command("discover-hyperliquid-hip4-targets")
def discover_hyperliquid_hip4_market_targets(
    max_markets: int = typer.Option(12, "--max-markets", help="Maximum HIP-4 outcome sides to keep."),
    include_orderbooks: bool = typer.Option(True, "--include-orderbooks/--no-orderbooks", help="Fetch l2Book for each outcome side."),
    latest_targets_path: Optional[Path] = typer.Option(None, "--latest-targets-path", help="Overwrite a concise Hyperliquid HIP-4 latest-targets markdown file."),
) -> None:
    """Find Hyperliquid HIP-4 outcome markets through public info endpoints."""
    result = discover_hyperliquid_hip4_targets(max_markets=max_markets, include_orderbooks=include_orderbooks)
    _render_hyperliquid_hip4_targets(result)
    if latest_targets_path:
        path = write_latest_hyperliquid_hip4_targets(result, latest_targets_path)
        console.print(f"Wrote latest Hyperliquid HIP-4 targets: {path}")


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


@app.command("evaluate-account-source")
def evaluate_account_source_command(
    handle: str = typer.Option("_FORAB", "--handle", help="X handle to evaluate as an ad-hoc signal source."),
    lookback: str = typer.Option("7d", "--lookback", help="Timeline lookback window, e.g. 24h, 7d, 30d."),
    accounts_csv: Optional[str] = typer.Option(None, "--accounts-csv", help="Optional CSV fallback for account profile rows."),
    posts_csv: Optional[str] = typer.Option(None, "--posts-csv", help="Optional CSV fallback for post rows."),
    max_posts: int = typer.Option(100, "--max-posts", help="Maximum X timeline posts requested."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate calls without contacting X API or writing reports."),
) -> None:
    """Evaluate a single public X account as a candidate source without changing watchlists."""
    normalized = handle.lstrip("@")
    con = connect()
    imported_accounts = import_accounts_csv(con, Path(accounts_csv)) if accounts_csv else 0
    imported_posts = import_posts_csv(con, Path(posts_csv)) if posts_csv else 0
    if imported_accounts or imported_posts:
        console.print(f"Imported {imported_accounts} account row(s) and {imported_posts} post row(s) from CSV.")

    token = os.getenv("X_BEARER_TOKEN")
    planned_calls = 0 if posts_csv else (2 if token else 0)
    cap = daily_cap or load_x_api_limits().daily_call_cap
    if token and not _check_x_budget(con, planned_calls, cap, dry_run, f"evaluate-account-source @{normalized} max_posts={max_posts}"):
        return
    if dry_run:
        console.print(
            f"Dry run only: would evaluate @{normalized}; planned_x_calls={planned_calls}; "
            f"csv_posts={'yes' if posts_csv else 'no'}."
        )
        return

    if token and not posts_csv:
        client = XApiClient(token)
        seconds = parse_duration(lookback)
        try:
            profile = client.user_profile(normalized)
            record_api_call(con, "x", "users/by/username", notes=f"profile @{normalized}")
            if profile:
                metrics = profile.get("public_metrics") or {}
                upsert_x_accounts(
                    con,
                    [
                        {
                            "handle": normalized,
                            "user_id": profile.get("id"),
                            "language": "mixed",
                            "role": "elite_information",
                            "region": "global",
                            "priority": "ad_hoc",
                            "followers": metrics.get("followers_count"),
                            "following": metrics.get("following_count"),
                            "verified": profile.get("verified"),
                            "profile_metrics": metrics,
                            "status": "active",
                            "notes": "Ad-hoc account source evaluation candidate.",
                        }
                    ],
                )
                save_raw_payload("x_account_profiles", normalized, profile)
            posts = client.backfill_user_post_dicts(normalized, seconds, max_results=max_posts)
            record_api_call(con, "x", "users/by/username", notes=f"resolve @{normalized}")
            record_api_call(con, "x", "users/:id/tweets", notes=f"timeline @{normalized}")
        except RequestException as exc:
            console.print(f"[yellow]warning: @{normalized} account evaluation sync failed: {exc}[/yellow]")
            posts = []
        if posts:
            save_raw_payload("x_account_posts", normalized, [post.get("raw_json") or post for post in posts])
            upsert_x_posts(con, posts)
    elif not token and not posts_csv:
        console.print(
            "[yellow]X_BEARER_TOKEN is not set and no --posts-csv was provided. "
            "Evaluating any existing local posts only.[/yellow]"
        )
        upsert_x_accounts(
            con,
            [
                {
                    "handle": normalized,
                    "language": "mixed",
                    "role": "elite_information",
                    "region": "global",
                    "priority": "ad_hoc",
                    "status": "active",
                    "notes": "Ad-hoc account source evaluation candidate.",
                }
            ],
        )

    result = evaluate_account_source(con, normalized, lookback)
    _render_account_source_evaluation(result)
    path = write_account_source_evaluation_report(result, SIGNAL_REPORT_ROOT)
    console.print(f"Wrote account source evaluation report: {path}")


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
    target: str = typer.Option("full", "--target", help="Price history target: full or event-windows."),
    pre: str = typer.Option("10m", "--pre", help="Event-window lookback when --target event-windows."),
    post: str = typer.Option("2h", "--post", help="Event-window lookahead when --target event-windows."),
    max_windows: int = typer.Option(200, "--max-windows", help="Maximum merged event windows to request."),
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
    if target not in {"full", "event-windows"}:
        console.print("[yellow]Unsupported target. Use full or event-windows.[/yellow]")
        return
    windows = [(start_dt, end_dt)] if target == "full" else event_price_windows(con, case, pre=pre, post=post, max_windows=max_windows)
    planned_calls = len(windows) * len(_chunks(token_rows, 20))
    console.print(
        f"Polymarket history plan: case={case}, market={case_row['market_slug']}, "
        f"tokens={len(token_rows)}, target={target}, windows={len(windows)}, requests={planned_calls}, "
        f"interval={interval}, fidelity={fidelity}"
    )
    if not windows:
        console.print("[yellow]No event windows found. Run backfill-x-history or match-event-posts first.[/yellow]")
        return
    if dry_run:
        console.print("Dry run only: no CLOB calls made.")
        return
    client = CLOBClient()
    total = 0
    for window_index, (window_start, window_end) in enumerate(windows):
        for chunk in _chunks(token_rows, 20):
            token_ids = [row["token_id"] for row in chunk]
            payload = client.batch_prices_history(
                token_ids,
                start_ts=int(window_start.timestamp()),
                end_ts=int(window_end.timestamp()),
                interval=interval,
                fidelity=fidelity,
            )
            save_raw_payload("polymarket_price_history", f"{case}_{target}_{window_index}", payload)
            token_to_market = {row["token_id"]: row for row in chunk}
            rows = normalize_price_history(payload, token_to_market, tick_source="historical_targeted" if target == "event-windows" else "historical")
            total += insert_historical_price_ticks(con, rows)
    console.print(f"Backfilled {total} historical price tick(s).")


@app.command("backfill-x-history")
def backfill_x_history(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    mode: str = typer.Option("x-api", "--mode", help="History source. V1 supports x-api."),
    daily_cap: Optional[int] = typer.Option(None, "--daily-cap", help="Local daily X API call cap."),
    max_accounts: Optional[int] = typer.Option(None, "--max-accounts", help="Maximum signal accounts to query."),
    max_posts_per_account: int = typer.Option(25, "--max-posts-per-account", help="Maximum posts requested per account."),
    handles: Optional[str] = typer.Option(None, "--handles", help="Comma-separated X handles to query instead of configured Web3 accounts."),
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
    accounts = _ad_hoc_accounts(handles) if handles else load_web3_accounts()
    selected = accounts[: max_accounts or load_x_api_limits().sync_accounts_max_accounts]
    planned_calls = len(selected) * 2
    cap = daily_cap or load_x_api_limits().daily_call_cap
    if not _check_x_budget(con, planned_calls, cap, dry_run, f"backfill-x-history case={case} accounts={len(selected)}"):
        return
    if dry_run:
        return

    client = XApiClient(token)
    keywords = list(
        dict.fromkeys(
            [
                *(case_row.get("keywords") or []),
                *case_keywords(str(case_row.get("query") or "")),
            ]
        )
    )
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
    horizons: str = typer.Option("", "--horizons", help="Comma-separated horizons. Defaults depend on mode."),
    mode: str = typer.Option("micro", "--mode", help="Backtest mode: micro, ramp, volatility, or event."),
) -> None:
    """Run post-level event-study backtest for an event case."""
    if mode not in {"micro", "ramp", "volatility", "event"}:
        console.print(f"[yellow]Unsupported mode '{mode}'. Use micro, ramp, volatility, or event.[/yellow]")
        return
    if horizons.strip():
        horizon_values = [part.strip() for part in horizons.split(",") if part.strip()]
    else:
        horizon_values = list(DEFAULT_MICRO_HORIZONS if mode == "micro" else DEFAULT_HORIZONS)
    impacts, metrics = run_event_backtest_case(connect(), case, horizon_values, mode=mode)
    console.print(f"Backtested {len(impacts)} {mode} post impact row(s); wrote {len(metrics)} account metric row(s).")


@app.command("match-event-posts")
def match_event_posts(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    method: str = typer.Option("semantic", "--method", help="Matching method: semantic or cloud."),
) -> None:
    """Match locally stored X posts to an event case with local semantic embeddings."""
    if method not in {"semantic", "cloud"}:
        console.print("[yellow]Unsupported method. V1 supports semantic or cloud.[/yellow]")
        return
    config = load_semantic_matching_config()
    if method == "cloud":
        result = match_event_posts_with_cloud_model(connect(), case, config)
    else:
        result = match_event_posts_semantically(connect(), case, config, method=method)
    if result.unavailable_reason:
        console.print(f"[yellow]{escape(result.unavailable_reason)}. Falling back to existing keyword-matched event posts.[/yellow]")
        return
    console.print(f"{method} matched {result.matches_written} post-market row(s); added {result.posts_added} event case post(s).")


@app.command("report-event-backtest")
def report_event_backtest(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
) -> None:
    """Write a local markdown historical event backtest report."""
    path = write_event_backtest_report(connect(), case, SIGNAL_REPORT_ROOT)
    console.print(f"Wrote event backtest report: {path}")


@app.command("mine-price-events")
def mine_price_events(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    windows: str = typer.Option(",".join(DEFAULT_PRICE_WINDOWS), "--windows", help="Comma-separated rolling windows."),
    min_move_pp: float = typer.Option(3.0, "--min-move-pp", help="Minimum absolute move in percentage points."),
) -> None:
    """Mine price-first repricing events from local market_ticks."""
    window_values = [part.strip() for part in windows.split(",") if part.strip()]
    events = mine_price_events_case(connect(), case, window_values, min_move_pp=min_move_pp)
    console.print(f"Mined {len(events)} price event(s) for {case}.")
    table = Table(title="Price Events")
    table.add_column("Start")
    table.add_column("Type")
    table.add_column("Direction")
    table.add_column("Move", justify="right")
    table.add_column("Resolution")
    table.add_column("Tags")
    for event in events[:20]:
        table.add_row(
            str(event.get("start_at")),
            str(event.get("event_type")),
            str(event.get("direction")),
            _fmt(event.get("move_size")),
            str(event.get("price_data_resolution")),
            ", ".join(event.get("risk_tags") or []),
        )
    console.print(table)


@app.command("plan-source-backfill")
def plan_source_backfill(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    pre: str = typer.Option("6h", "--pre", help="Lookback before price event start."),
    post: str = typer.Option("30m", "--post", help="Lookahead after price event start."),
    platform: str = typer.Option("x", "--platform", help="Source platform. V1 supports x."),
    use_counts: bool = typer.Option(True, "--use-counts/--no-counts", help="Use X counts preflight when credentials are available."),
    daily_cap: int = typer.Option(200, "--daily-cap", help="Local daily X call cap."),
    max_count: int = typer.Option(500, "--max-count", help="Mark plans too expensive above this count."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show planning result without writing plans or calling X."),
) -> None:
    """Plan bounded source searches around mined price events."""
    if platform != "x":
        console.print(f"[yellow]Unsupported platform '{platform}'. V1 supports x.[/yellow]")
        return
    con = connect()
    events = price_events_for_case(con, case)
    token = os.getenv("X_BEARER_TOKEN")
    planned_calls = len(events) if use_counts and token else 0
    if use_counts and token and not _check_x_budget(con, planned_calls, daily_cap, dry_run, f"plan-source-backfill case={case}"):
        return
    count_provider = None
    if use_counts and token and not dry_run:
        client = XApiClient(token)

        def count_provider(query: str, start_time: str, end_time: str) -> dict:
            payload = client.full_archive_counts(query, start_time, end_time)
            record_api_call(con, "x", "tweets/counts/all", notes=f"price-first case={case}")
            return payload

    try:
        plans = plan_source_backfill_case(
            con,
            case,
            pre=pre,
            post=post,
            platform=platform,
            use_counts=use_counts,
            daily_cap=daily_cap,
            max_count=max_count,
            count_provider=count_provider,
            write=not dry_run,
        )
    except RequestException as exc:
        if _http_status(exc) in {401, 403}:
            console.print("[yellow]X full-archive counts are unavailable for this token. Plan without --use-counts or upgrade X access.[/yellow]")
            return
        raise
    console.print(f"Planned {len(plans)} source backfill window(s) for {case}.")
    table = Table(title="Source Backfill Plans")
    table.add_column("Status")
    table.add_column("Event")
    table.add_column("Calls", justify="right")
    table.add_column("Reason")
    for plan in plans[:30]:
        table.add_row(str(plan.get("status")), str(plan.get("price_event_id")), _fmt(plan.get("planned_calls")), str(plan.get("reason") or ""))
    console.print(table)


@app.command("match-price-events")
def match_price_events(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    method: str = typer.Option("keyword", "--method", help="Matching method: keyword or cloud."),
    min_confidence: float = typer.Option(0.70, "--min-confidence", help="Minimum match confidence."),
) -> None:
    """Match local posts to mined price events."""
    if method not in {"keyword", "cloud"}:
        console.print("[yellow]Unsupported method. Use keyword or cloud.[/yellow]")
        return
    matches = match_price_events_case(connect(), case, method=method, min_confidence=min_confidence)
    console.print(f"Matched {len(matches)} post-price-event row(s) for {case}.")


@app.command("run-price-first-backtest")
def run_price_first_backtest(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
    horizons: str = typer.Option(",".join(DEFAULT_PRICE_FIRST_HORIZONS), "--horizons", help="Comma-separated horizons."),
    execution: str = typer.Option("top-of-book", "--execution", help="Execution assumption. V1 supports top-of-book."),
) -> None:
    """Run price-first historical backtest from matched post/price-event rows."""
    if execution != "top-of-book":
        console.print("[yellow]Unsupported execution mode. V1 supports top-of-book.[/yellow]")
        return
    horizon_values = [part.strip() for part in horizons.split(",") if part.strip()]
    run_id, samples, metrics = run_price_first_backtest_case(connect(), case, horizon_values, execution=execution)
    console.print(f"Price-first run {run_id}: wrote {len(samples)} sample row(s), {len(metrics)} account metric row(s).")


@app.command("report-price-first-backtest")
def report_price_first_backtest(
    case: str = typer.Option(..., "--case", help="Historical event case id."),
) -> None:
    """Write a local markdown price-first historical backtest report."""
    path = write_price_first_report(connect(), case, SIGNAL_REPORT_ROOT)
    console.print(f"Wrote price-first backtest report: {path}")


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
        insert_market_tick(
            con,
            parse_timestamp(payload.get("timestamp")) or utc_now_iso(),
            market_slug,
            token_id,
            best_bid,
            best_ask,
            last_trade,
            None,
            payload,
            tick_source="live_websocket",
        )

    CLOBWebSocket().stream(list(token_to_market), on_message=on_message, seconds=seconds)
    console.print(f"Streamed CLOB updates for {seconds}s from {len(token_to_market)} token(s).")


@app.command("sync-market-ticks")
def sync_market_ticks(
    category: str = typer.Option("crypto", "--category", help="Market category filter: crypto, politics, or all."),
    case: Optional[str] = typer.Option(None, "--case", help="Historical event case id. Overrides category/max-markets."),
    max_markets: int = typer.Option(20, "--max-markets", help="Maximum markets to snapshot."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Estimate token snapshots without calling CLOB."),
) -> None:
    """Snapshot public CLOB midpoint prices for discovered markets."""
    con = connect()
    markets, token_rows = _market_tick_plan(con, category, max_markets, case=case)
    scope = f"case={case}" if case else f"category={category}"
    console.print(f"Polymarket CLOB plan: {scope}, markets={len(markets)}, token_midpoints={len(token_rows)}")
    if dry_run:
        console.print("Dry run only: no CLOB calls made.")
        return
    count = _snapshot_market_ticks(con, token_rows)
    console.print(f"Synced {count} CLOB midpoint tick(s).")


@app.command("collect-market-ticks")
def collect_market_ticks(
    category: str = typer.Option("all", "--category", help="Market category filter: crypto, politics, or all."),
    case: Optional[str] = typer.Option(None, "--case", help="Historical event case id. Overrides category/max-markets."),
    max_markets: int = typer.Option(20, "--max-markets", help="Maximum markets to snapshot per iteration."),
    interval_seconds: int = typer.Option(300, "--interval-seconds", help="Seconds between midpoint snapshots."),
    iterations: int = typer.Option(12, "--iterations", help="Number of snapshot iterations. Use a bounded value."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show collection plan without calling CLOB."),
) -> None:
    """Continuously collect bounded Polymarket CLOB midpoint snapshots."""
    con = connect()
    markets, token_rows = _market_tick_plan(con, category, max_markets, case=case)
    planned_calls = len(token_rows) * max(0, iterations)
    scope = f"case={case}" if case else f"category={category}"
    console.print(
        f"Polymarket CLOB continuous plan: {scope}, markets={len(markets)}, "
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
        console.print("[yellow]No token ids found. Run discover-markets/discover-event-case first.[/yellow]")
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


@app.command("collect-market-burst")
def collect_market_burst(
    case: Optional[str] = typer.Option(None, "--case", help="Historical/live event case id."),
    cases: Optional[str] = typer.Option(None, "--cases", help="Comma-separated event case ids. Overrides --case."),
    trigger_post_id: Optional[str] = typer.Option(None, "--trigger-post-id", help="Optional triggering post id for run de-duplication."),
    trigger_handle: Optional[str] = typer.Option(None, "--trigger-handle", help="Optional triggering handle for audit records."),
    trigger_confidence: Optional[float] = typer.Option(None, "--trigger-confidence", help="Optional trigger confidence for audit records."),
    fast_seconds: int = typer.Option(60, "--fast-seconds", help="Initial 1s collection duration."),
    medium_seconds: int = typer.Option(600, "--medium-seconds", help="10s collection duration after fast phase."),
    slow_seconds: int = typer.Option(6600, "--slow-seconds", help="60s collection duration after medium phase."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show bounded burst plan without calling CLOB."),
) -> None:
    """Collect bounded post-event burst midpoint snapshots for one case."""
    con = connect()
    case_ids = _case_ids_from_options(case, cases)
    if not case_ids:
        console.print("[yellow]Provide --case or --cases.[/yellow]")
        return
    phases = [
        ("1s", 1, max(0, fast_seconds)),
        ("10s", 10, max(0, medium_seconds)),
        ("1m", 60, max(0, slow_seconds)),
    ]
    iterations = [(label, interval, seconds // interval if interval else 0) for label, interval, seconds in phases]
    plans = []
    for case_id in case_ids:
        markets, token_rows = _market_tick_plan(con, "all", 1, case=case_id)
        plans.append((case_id, markets, token_rows))
    planned_calls = sum(len(token_rows) * sum(count for _, _, count in iterations) for _, _, token_rows in plans)
    console.print(
        f"Polymarket burst plan: cases={len(case_ids)}, "
        f"phases={iterations}, planned_calls={planned_calls}"
    )
    if dry_run:
        console.print("Dry run only: no CLOB calls made.")
        return
    for case_id, markets, token_rows in plans:
        total += _collect_burst_for_case(
            con,
            case_id,
            token_rows,
            iterations,
            trigger_post_id=trigger_post_id,
            trigger_handle=trigger_handle,
            trigger_confidence=trigger_confidence,
        )
    console.print(f"Collected {total} burst midpoint tick(s).")


@app.command("monitor-event-live")
def monitor_event_live(
    cases: str = typer.Option(..., "--cases", help="Comma-separated event case ids."),
    poll_seconds: int = typer.Option(60, "--poll-seconds", help="Seconds between monitor loops."),
    iterations: int = typer.Option(1, "--iterations", help="Bounded monitor loop count."),
    max_handles: int = typer.Option(17, "--max-handles", help="Maximum X handles to backfill per loop."),
    max_posts_per_handle: int = typer.Option(20, "--max-posts-per-handle", help="Maximum posts per handle per loop."),
    daily_x_cap: int = typer.Option(200, "--daily-x-cap", help="Local daily X API call cap."),
    min_confidence: float = typer.Option(0.70, "--min-confidence", help="Minimum cloud match confidence to trigger burst."),
    max_trigger_age: str = typer.Option("10m", "--max-trigger-age", help="Only trigger burst from posts newer than this duration."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show monitor actions without calling external APIs or writing burst runs."),
) -> None:
    """Monitor event cases and trigger live micro burst collection from high-confidence cloud matches."""
    con = connect()
    case_ids = _case_ids_from_options(None, cases)
    if not case_ids:
        console.print("[yellow]No cases supplied.[/yellow]")
        return
    max_trigger_age_seconds = parse_duration(max_trigger_age)
    for loop_index in range(max(1, iterations)):
        loop_started_at = utc_now_iso()
        min_post_created_at = (datetime.now(timezone.utc) - timedelta(seconds=max_trigger_age_seconds)).isoformat()
        baseline_calls = 0
        for case_id in case_ids:
            _, token_rows = _market_tick_plan(con, "all", 1, case=case_id)
            baseline_calls += len(token_rows)
            if not dry_run:
                _snapshot_market_ticks(con, token_rows, tick_source="live_baseline")
        console.print(f"Monitor loop {loop_index + 1}/{max(1, iterations)}: baseline_calls={baseline_calls}")

        if dry_run:
            console.print(
                f"Dry run only: would sync-social max_handles={max_handles}, max_posts={max_posts_per_handle}, daily_x_cap={daily_x_cap}; "
                f"would cloud-match cases={len(case_ids)} and trigger confidence>={min_confidence}, post_age<={max_trigger_age}."
            )
        else:
            _sync_social_watchlist(con, backfill="24h", max_handles=max_handles, max_posts_per_handle=max_posts_per_handle, daily_cap=daily_x_cap)
            for case_id in case_ids:
                result = match_event_posts_with_cloud_model(con, case_id, load_semantic_matching_config())
                if result.unavailable_reason:
                    console.print(f"[yellow]{escape(result.unavailable_reason)}. Skipping cloud triggers for {case_id}.[/yellow]")
                else:
                    console.print(f"{case_id}: cloud matched {result.matches_written}; added {result.posts_added} event posts.")

        candidates = live_burst_trigger_candidates(
            con,
            case_ids,
            min_confidence,
            limit=1,
            min_match_created_at=None if dry_run else loop_started_at,
            min_post_created_at=min_post_created_at,
        )
        if not candidates:
            console.print("No new high-confidence burst trigger.")
        elif dry_run:
            candidate = candidates[0]
            console.print(
                f"Dry run trigger candidate: case={candidate['case_id']} post={candidate['post_id']} "
                f"@{candidate['handle']} confidence={candidate['similarity']}"
            )
        else:
            candidate = candidates[0]
            _, token_rows = _market_tick_plan(con, "all", 1, case=candidate["case_id"])
            phases = [("1s", 1, 60), ("10s", 10, 60), ("1m", 60, 110)]
            written = _collect_burst_for_case(
                con,
                candidate["case_id"],
                token_rows,
                phases,
                trigger_post_id=str(candidate["post_id"]),
                trigger_handle=str(candidate.get("handle") or ""),
                trigger_confidence=float(candidate.get("similarity") or 0),
            )
            console.print(f"Triggered burst for {candidate['case_id']}/{candidate['post_id']}: ticks={written}")
            run_event_backtest_case(con, candidate["case_id"], list(DEFAULT_MICRO_HORIZONS), mode="micro")
            path = write_event_backtest_report(con, candidate["case_id"], SIGNAL_REPORT_ROOT)
            console.print(f"Updated event report: {path}")
        if loop_index < max(1, iterations) - 1:
            time.sleep(max(0, poll_seconds))


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


def _ad_hoc_accounts(handles: Optional[str]):
    from .config import Web3AccountConfig

    if not handles:
        return []
    accounts = []
    for handle in handles.split(","):
        normalized = handle.strip().lstrip("@")
        if normalized:
            accounts.append(Web3AccountConfig(normalized, "mixed", "global", "elite_information", "ad_hoc"))
    return accounts


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


def _market_tick_plan(con, category: str, max_markets: int, case: Optional[str] = None):
    if case:
        markets = _markets_for_case_tick_sync(con, case)
    else:
        markets = _markets_for_tick_sync(con, category, max_markets)
    token_rows = []
    for market in markets:
        for token_id in _json_list(market.get("clob_token_ids"))[:1]:
            token_rows.append((market["market_slug"], token_id, market.get("liquidity")))
    return markets, token_rows


def _markets_for_case_tick_sync(con, case: str) -> list[dict]:
    case_row = get_event_case(con, case)
    if not case_row or not case_row.get("market_slug"):
        return []
    row = con.execute(
        """
        select market_slug, event_slug, question, category, tags, end_time, clob_token_ids, liquidity
        from markets
        where market_slug = ?
        """,
        [case_row["market_slug"]],
    ).fetchone()
    if not row:
        return []
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row))]


def _collect_burst_for_case(
    con,
    case_id: str,
    token_rows,
    iterations,
    trigger_post_id: Optional[str] = None,
    trigger_handle: Optional[str] = None,
    trigger_confidence: Optional[float] = None,
) -> int:
    if not token_rows:
        console.print(f"[yellow]No token ids found for {case_id}. Run discover-event-case first.[/yellow]")
        return 0
    if trigger_post_id and live_burst_run_exists(con, case_id, trigger_post_id):
        console.print(f"[yellow]Skipping {case_id}/{trigger_post_id}: burst already recorded.[/yellow]")
        return 0
    run_id = stable_hash([case_id, trigger_post_id or utc_now_iso()])[:24]
    planned_calls = len(token_rows) * sum(count for _, _, count in iterations)
    if trigger_post_id:
        upsert_live_burst_run(
            con,
            {
                "run_id": run_id,
                "case_id": case_id,
                "post_id": trigger_post_id,
                "handle": trigger_handle,
                "confidence": trigger_confidence,
                "status": "running",
                "planned_calls": planned_calls,
                "ticks_written": 0,
            },
        )
    total = 0
    error = None
    for label, interval, count in iterations:
        for index in range(count):
            try:
                written = _snapshot_market_ticks(con, token_rows, tick_source="live_burst")
            except Exception as exc:  # defensive: keep monitor alive if one snapshot fails
                written = 0
                error = str(exc)
                console.print(f"[yellow]warning: burst failed for {case_id}: {exc}[/yellow]")
            total += written
            console.print(f"Burst {case_id} {label} {index + 1}/{count}: synced {written} midpoint tick(s).")
            if index < count - 1:
                time.sleep(interval)
    if trigger_post_id:
        upsert_live_burst_run(
            con,
            {
                "run_id": run_id,
                "case_id": case_id,
                "post_id": trigger_post_id,
                "handle": trigger_handle,
                "confidence": trigger_confidence,
                "status": "completed" if error is None and total >= planned_calls else "partial",
                "planned_calls": planned_calls,
                "ticks_written": total,
                "error": error,
            },
        )
    return total


def _sync_social_watchlist(con, backfill: str, max_handles: int, max_posts_per_handle: int, daily_cap: int) -> int:
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        console.print("[yellow]X_BEARER_TOKEN is not set. Skipping sync-social.[/yellow]")
        return 0
    handles = load_social_handles()[:max_handles]
    planned_calls = len(handles) * 2
    if not _check_x_budget(con, planned_calls, daily_cap, False, f"monitor sync-social handles={len(handles)} max_posts={max_posts_per_handle}"):
        return 0
    client = XApiClient(token)
    seconds = parse_duration(backfill)
    total = 0
    for handle in handles:
        try:
            posts = client.backfill_user_posts(handle.handle, seconds, max_results=max_posts_per_handle)
            record_api_call(con, "x", "users/by/username", notes=f"resolve @{handle.handle}")
            record_api_call(con, "x", "users/:id/tweets", notes=f"timeline @{handle.handle}")
        except RequestException as exc:
            console.print(f"[yellow]warning: @{handle.handle} monitor backfill failed: {exc}[/yellow]")
            continue
        upsert_social_posts(con, posts)
        if posts:
            save_raw_payload("x_posts", handle.handle, [post.raw for post in posts])
        total += len(posts)
    console.print(f"Monitor synced {total} X post(s).")
    return total


def _case_ids_from_options(case: Optional[str], cases: Optional[str]) -> list[str]:
    if cases:
        return [part.strip() for part in cases.split(",") if part.strip()]
    return [case] if case else []


def _snapshot_market_ticks(con, token_rows, tick_source: str = "live") -> int:
    client = CLOBClient()
    observed_at = utc_now_iso()
    count = 0
    for market_slug, token_id, liquidity in token_rows:
        try:
            mid = client.midpoint(token_id)
        except RequestException as exc:
            console.print(f"[yellow]warning: midpoint failed for {market_slug}/{token_id}: {exc}[/yellow]")
            continue
        if tick_source == "live":
            insert_market_midpoint_tick(con, observed_at, market_slug, token_id, mid, liquidity, {"mid": mid, "token_id": token_id})
        else:
            insert_market_midpoint_tick_with_source(
                con,
                observed_at,
                market_slug,
                token_id,
                mid,
                liquidity,
                {"mid": mid, "token_id": token_id, "tick_source": tick_source},
                tick_source=tick_source,
            )
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


def _render_signal_discovery(result: dict) -> None:
    market_table = Table(title="High-Interest Open Polymarket Pools")
    market_table.add_column("Rank", justify="right")
    market_table.add_column("Market")
    market_table.add_column("Score", justify="right")
    market_table.add_column("Liquidity", justify="right")
    market_table.add_column("Volume", justify="right")
    market_table.add_column("Terms")
    for idx, item in enumerate((result.get("markets") or [])[:20], start=1):
        record = item["record"]
        market_table.add_row(
            str(idx),
            str(record.market_slug),
            _fmt(item.get("score")),
            _fmt(item.get("liquidity")),
            _fmt(item.get("volume")),
            ", ".join(item.get("query_terms") or []),
        )
    console.print(market_table)

    seed_table = Table(title="Public Seed Preflight")
    seed_table.add_column("Rank", justify="right")
    seed_table.add_column("Platform")
    seed_table.add_column("Source")
    seed_table.add_column("Market")
    seed_table.add_column("Score", justify="right")
    seed_table.add_column("Status")
    seed_table.add_column("Risk")
    for idx, row in enumerate((result.get("public_seed_candidates") or [])[:30], start=1):
        handle = str(row.get("handle") or "")
        label = f"@{handle}" if row.get("platform") == "x" else handle
        seed_table.add_row(
            str(idx),
            str(row.get("platform") or ""),
            label,
            str(row.get("market_slug") or ""),
            _fmt(row.get("discovery_score")),
            str(row.get("recommended_status") or ""),
            ", ".join((row.get("risk_tags") or [])[:3]),
        )
    console.print(seed_table)

    source_table = Table(title="Discovered Signal Source Candidates")
    source_table.add_column("Rank", justify="right")
    source_table.add_column("Handle")
    source_table.add_column("Market")
    source_table.add_column("Score", justify="right")
    source_table.add_column("Posts", justify="right")
    source_table.add_column("Engagement", justify="right")
    source_table.add_column("Status")
    source_table.add_column("Edge")
    source_table.add_column("Tradability")
    for idx, row in enumerate((result.get("source_candidates") or [])[:30], start=1):
        tradability = row.get("tradability") if isinstance(row.get("tradability"), dict) else {}
        source_table.add_row(
            str(idx),
            f"@{row.get('handle')}",
            str(row.get("market_slug") or ""),
            _fmt(row.get("discovery_score")),
            str(row.get("post_count") or 0),
            _fmt(row.get("engagement_score")),
            str(row.get("recommended_status") or ""),
            str(row.get("edge_classification") or ""),
            str(tradability.get("status") or ""),
        )
    console.print(source_table)

    kalshi_table = Table(title="Kalshi Hot Pool Context")
    kalshi_table.add_column("Rank", justify="right")
    kalshi_table.add_column("Category")
    kalshi_table.add_column("Ticker")
    kalshi_table.add_column("Heat", justify="right")
    kalshi_table.add_column("24h Vol", justify="right")
    kalshi_table.add_column("Spread", justify="right")
    kalshi_table.add_column("Terms")
    for idx, row in enumerate((result.get("kalshi_hot_markets") or [])[:20], start=1):
        kalshi_table.add_row(
            str(idx),
            str(row.get("category") or ""),
            str(row.get("ticker") or ""),
            _fmt(row.get("heat_score")),
            _fmt(row.get("volume_24h")),
            _fmt(row.get("spread")),
            ", ".join(row.get("query_terms") or []),
        )
    console.print(kalshi_table)

    cross_venue_table = Table(title="Kalshi Cross-Venue Matches")
    cross_venue_table.add_column("Rank", justify="right")
    cross_venue_table.add_column("Kalshi")
    cross_venue_table.add_column("Polymarket")
    cross_venue_table.add_column("Score", justify="right")
    cross_venue_table.add_column("Status")
    cross_venue_table.add_column("Risk")
    for idx, row in enumerate((result.get("kalshi_cross_venue") or [])[:20], start=1):
        cross_venue_table.add_row(
            str(idx),
            str(row.get("kalshi_ticker") or row.get("handle") or ""),
            str(row.get("market_slug") or ""),
            _fmt(row.get("discovery_score")),
            str(row.get("recommended_status") or ""),
            ", ".join((row.get("risk_tags") or [])[:3]),
        )
    console.print(cross_venue_table)


def _render_kalshi_targets(result: dict) -> None:
    table = Table(title="High-Interest Open Kalshi Markets")
    table.add_column("Rank", justify="right")
    table.add_column("Category")
    table.add_column("Ticker")
    table.add_column("Score", justify="right")
    table.add_column("Volume", justify="right")
    table.add_column("Open Interest", justify="right")
    table.add_column("Spread", justify="right")
    table.add_column("Terms")
    for idx, item in enumerate((result.get("markets") or [])[:30], start=1):
        record = item.get("record") or {}
        table.add_row(
            str(idx),
            str(item.get("category") or ""),
            str(record.get("ticker") or ""),
            _fmt(item.get("score")),
            _fmt(item.get("volume")),
            _fmt(item.get("open_interest")),
            _fmt(item.get("spread")),
            ", ".join(item.get("query_terms") or []),
        )
    console.print(table)


def _render_hyperliquid_hip4_targets(result: dict) -> None:
    table = Table(title="Hyperliquid HIP-4 Outcome Markets")
    table.add_column("Rank", justify="right")
    table.add_column("Coin")
    table.add_column("Side")
    table.add_column("Category")
    table.add_column("Score", justify="right")
    table.add_column("Mid", justify="right")
    table.add_column("Bid", justify="right")
    table.add_column("Ask", justify="right")
    table.add_column("Spread", justify="right")
    table.add_column("Descriptor")
    for idx, item in enumerate((result.get("markets") or [])[:30], start=1):
        descriptor = item.get("descriptor") if isinstance(item.get("descriptor"), dict) else {}
        descriptor_text = ",".join(
            f"{key}:{value}" for key, value in descriptor.items() if key in {"class", "underlying", "expiry", "targetPrice"}
        )
        table.add_row(
            str(idx),
            str(item.get("coin") or ""),
            str(item.get("side") or ""),
            str(item.get("category") or ""),
            _fmt(item.get("score")),
            _fmt(item.get("mid")),
            _fmt(item.get("best_bid")),
            _fmt(item.get("best_ask")),
            _fmt(item.get("spread")),
            descriptor_text,
        )
    console.print(table)


def _render_account_source_evaluation(result: dict) -> None:
    metric = result.get("metric") if isinstance(result.get("metric"), dict) else {}
    tradability = result.get("tradability") if isinstance(result.get("tradability"), dict) else {}
    table = Table(title=f"Account Source Evaluation @{result.get('handle')}")
    table.add_column("Posts", justify="right")
    table.add_column("Market Links", justify="right")
    table.add_column("Outcomes", justify="right")
    table.add_column("Final", justify="right")
    table.add_column("Market Impact", justify="right")
    table.add_column("False FOMO", justify="right")
    table.add_column("Status")
    table.add_column("Tradability")
    table.add_row(
        str(result.get("post_count") or 0),
        str(result.get("market_link_count") or 0),
        str(result.get("outcome_count") or 0),
        _fmt(metric.get("final_score")),
        _fmt(metric.get("market_impact_score")),
        _fmt(metric.get("false_fomo_rate")),
        str(metric.get("recommended_status") or "insufficient_x_data"),
        str(tradability.get("status") or "unknown"),
    )
    console.print(table)

    markets = result.get("market_counts") if isinstance(result.get("market_counts"), dict) else {}
    if markets:
        market_table = Table(title="Matched Markets")
        market_table.add_column("Market")
        market_table.add_column("Count", justify="right")
        for market, count in list(markets.items())[:10]:
            market_table.add_row(str(market), str(count))
        console.print(market_table)


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
