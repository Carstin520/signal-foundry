from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import duckdb

from quant_sol.wallets.config import DB_PATH

from .config import SIGNAL_RAW_ROOT
from .models import EventMention, MarketRecord, SignalScore, SocialPost
from .utils import stable_hash, utc_now_iso


def connect(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    ensure_schema(con)
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table if not exists markets (
            market_slug varchar primary key,
            event_slug varchar,
            question varchar,
            category varchar,
            tags json,
            end_time timestamp,
            resolution_source varchar,
            clob_token_ids json,
            liquidity double,
            updated_at timestamp not null,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists market_ticks (
            observed_at timestamp not null,
            market_slug varchar,
            token_id varchar,
            best_bid double,
            best_ask double,
            mid double,
            spread double,
            last_trade_price double,
            liquidity double,
            tick_source varchar,
            ingested_at timestamp,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists wallet_activity (
            wallet varchar not null,
            market_slug varchar,
            market_id varchar,
            side varchar,
            price double,
            size double,
            notional double,
            activity_ts timestamp,
            tx_hash varchar,
            fetched_at timestamp not null,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists social_posts (
            platform varchar not null,
            handle varchar not null,
            post_id varchar not null,
            created_at timestamp not null,
            text varchar,
            url varchar,
            raw_json_hash varchar not null,
            raw_json json,
            primary key (platform, post_id)
        )
        """
    )
    con.execute(
        """
        create table if not exists x_accounts (
            handle varchar primary key,
            user_id varchar,
            language varchar,
            role varchar,
            region varchar,
            priority varchar,
            followers integer,
            following integer,
            verified boolean,
            profile_metrics json,
            status varchar,
            notes varchar,
            updated_at timestamp not null
        )
        """
    )
    con.execute(
        """
        create table if not exists x_posts (
            post_id varchar primary key,
            handle varchar not null,
            created_at timestamp not null,
            text varchar,
            public_metrics json,
            referenced_tweets json,
            lang varchar,
            raw_json_hash varchar not null,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists x_follow_graph (
            source_handle varchar not null,
            target_handle varchar not null,
            relationship varchar not null,
            collected_at timestamp not null,
            primary key (source_handle, target_handle, relationship)
        )
        """
    )
    con.execute(
        """
        create table if not exists account_narrative_mentions (
            account varchar not null,
            narrative_key varchar not null,
            first_seen_at timestamp,
            post_count integer,
            matched_markets json,
            primary key (account, narrative_key)
        )
        """
    )
    con.execute(
        """
        create table if not exists account_impact_metrics (
            account varchar not null,
            lookback varchar not null,
            speed_score double,
            frequency_score double,
            cascade_score double,
            market_impact_score double,
            source_chain_score double,
            false_fomo_rate double,
            final_score double,
            recommended_status varchar,
            sample_size integer,
            market_link_coverage double,
            hit_rate_6h double,
            hit_rate_24h double,
            hit_rate_72h double,
            avg_favorable_move double,
            avg_adverse_move double,
            evaluated_at timestamp not null,
            primary key (account, lookback)
        )
        """
    )
    con.execute(
        """
        create table if not exists account_source_chains (
            downstream_account varchar not null,
            upstream_account varchar not null,
            evidence_type varchar,
            lead_time_minutes double,
            shared_narratives json,
            confidence double,
            primary key (downstream_account, upstream_account, evidence_type)
        )
        """
    )
    con.execute(
        """
        create table if not exists signal_discovery_sources (
            run_id varchar not null,
            platform varchar not null,
            handle varchar not null,
            market_slug varchar not null,
            first_seen_at timestamp,
            post_count integer,
            engagement_score double,
            discovery_score double,
            recommended_status varchar,
            evidence json,
            created_at timestamp not null,
            primary key (run_id, platform, handle, market_slug)
        )
        """
    )
    con.execute(
        """
        create table if not exists account_market_mentions (
            account varchar not null,
            post_id varchar not null,
            market_slug varchar not null,
            narrative_key varchar not null,
            entity varchar not null,
            confidence double,
            direction varchar,
            post_created_at timestamp,
            primary key (post_id, market_slug, narrative_key, entity)
        )
        """
    )
    con.execute(
        """
        create table if not exists account_market_outcomes (
            account varchar not null,
            post_id varchar not null,
            market_slug varchar not null,
            horizon varchar not null,
            entry_mid double,
            future_mid double,
            delta double,
            max_favorable_delta double,
            max_adverse_delta double,
            is_positive boolean,
            evaluated_at timestamp not null,
            primary key (post_id, market_slug, horizon)
        )
        """
    )
    con.execute(
        """
        create table if not exists event_mentions (
            post_id varchar not null,
            market_slug varchar not null,
            event_slug varchar,
            entities json,
            keywords json,
            confidence double,
            created_at timestamp not null,
            primary key (post_id, market_slug)
        )
        """
    )
    con.execute(
        """
        create table if not exists narrative_snapshots (
            snapshot_at timestamp not null,
            event_family varchar,
            market_slug varchar not null,
            window_label varchar not null,
            post_count integer,
            weighted_post_count double,
            unique_handles integer,
            source_categories json,
            top_keywords json,
            direction varchar,
            sentiment_strength double,
            primary key (snapshot_at, market_slug, window_label)
        )
        """
    )
    con.execute(
        """
        create table if not exists market_fomo_state (
            snapshot_at timestamp not null,
            market_slug varchar not null,
            mid double,
            spread double,
            liquidity double,
            price_band varchar,
            move_1h double,
            move_6h double,
            move_24h double,
            deadline_days double,
            fomo_capacity double,
            primary key (snapshot_at, market_slug)
        )
        """
    )
    con.execute(
        """
        create table if not exists signal_events (
            signal_id varchar primary key,
            generated_at timestamp not null,
            event_family varchar,
            market_slug varchar,
            direction_hint varchar,
            score integer,
            confidence varchar,
            evidence json,
            risk_tags json,
            source_posts json,
            wallet_flows json,
            price_window json
        )
        """
    )
    con.execute(
        """
        create table if not exists signal_outcomes (
            signal_id varchar not null,
            horizon varchar not null,
            entry_mid double,
            future_mid double,
            delta double,
            max_favorable_delta double,
            max_adverse_delta double,
            overshoot boolean,
            evaluated_at timestamp not null,
            primary key (signal_id, horizon)
        )
        """
    )
    con.execute(
        """
        create table if not exists telegram_alerts (
            signal_id varchar not null,
            sent_at timestamp not null,
            status varchar not null,
            payload varchar,
            error varchar
        )
        """
    )
    con.execute(
        """
        create table if not exists event_cases (
            case_id varchar primary key,
            query varchar not null,
            market_slug varchar,
            start_at timestamp,
            end_at timestamp,
            keywords json,
            status varchar,
            created_at timestamp not null,
            updated_at timestamp not null
        )
        """
    )
    con.execute(
        """
        create table if not exists event_case_posts (
            case_id varchar not null,
            post_id varchar not null,
            handle varchar not null,
            created_at timestamp not null,
            text varchar,
            direction varchar,
            matched_keywords json,
            raw_json_hash varchar not null,
            raw_json json,
            primary key (case_id, post_id)
        )
        """
    )
    con.execute(
        """
        create table if not exists post_market_semantic_matches (
            case_id varchar not null,
            post_id varchar not null,
            handle varchar not null,
            market_slug varchar not null,
            method varchar not null,
            similarity double,
            matched_concepts json,
            rejected_concepts json,
            decision varchar,
            created_at timestamp not null,
            primary key (case_id, post_id, market_slug, method)
        )
        """
    )
    con.execute(
        """
        create table if not exists live_burst_runs (
            run_id varchar primary key,
            case_id varchar not null,
            post_id varchar not null,
            handle varchar,
            triggered_at timestamp not null,
            confidence double,
            status varchar not null,
            planned_calls integer,
            ticks_written integer,
            error varchar,
            updated_at timestamp not null,
            unique (case_id, post_id)
        )
        """
    )
    con.execute(
        """
        create table if not exists event_post_impacts (
            case_id varchar not null,
            post_id varchar not null,
            handle varchar not null,
            market_slug varchar not null,
            horizon varchar not null,
            mode varchar,
            entry_mid double,
            future_mid double,
            delta double,
            close_delta double,
            max_favorable_delta double,
            max_adverse_delta double,
            entry_delay_seconds double,
            price_in_time timestamp,
            ramp_duration_minutes double,
            price_move_started_before_post boolean,
            is_positive boolean,
            is_strong boolean,
            tradable_ramp boolean,
            strong_ramp boolean,
            already_hot_penalty boolean,
            crowded_entry boolean,
            late_stage_ramp boolean,
            price_data_resolution varchar,
            time_to_3pp_seconds double,
            max_move_10m double,
            reversal_30m double,
            execution_cost double,
            net_close_delta double,
            net_max_favorable_delta double,
            net_max_adverse_delta double,
            paper_trade_positive boolean,
            paper_trade_strong boolean,
            risk_tags json,
            evaluated_at timestamp not null,
            primary key (case_id, post_id, market_slug, horizon)
        )
        """
    )
    con.execute(
        """
        create table if not exists event_account_metrics (
            account varchar not null,
            case_id varchar not null,
            lead_score double,
            impact_score double,
            hit_rate double,
            false_fomo_rate double,
            sample_size integer,
            ramp_hit_rate double,
            strong_ramp_rate double,
            avg_entry_delay_seconds double,
            avg_max_favorable_delta double,
            avg_max_adverse_delta double,
            already_hot_rate double,
            tradable_score double,
            micro_hit_rate double,
            median_time_to_price_in double,
            sub_10m_hit_rate double,
            late_after_price_move_rate double,
            recommended_status varchar,
            evaluated_at timestamp not null,
            primary key (account, case_id)
        )
        """
    )
    con.execute(
        """
        create table if not exists api_usage_log (
            service varchar not null,
            endpoint varchar not null,
            called_at timestamp not null,
            call_count integer not null,
            notes varchar
        )
        """
    )
    _ensure_columns(
        con,
        "account_impact_metrics",
        {
            "sample_size": "integer",
            "market_link_coverage": "double",
            "hit_rate_6h": "double",
            "hit_rate_24h": "double",
            "hit_rate_72h": "double",
            "avg_favorable_move": "double",
            "avg_adverse_move": "double",
        },
    )
    _ensure_columns(
        con,
        "market_ticks",
        {
            "tick_source": "varchar",
            "ingested_at": "timestamp",
        },
    )
    _ensure_columns(
        con,
        "event_post_impacts",
        {
            "mode": "varchar",
            "close_delta": "double",
            "entry_delay_seconds": "double",
            "price_in_time": "timestamp",
            "ramp_duration_minutes": "double",
            "tradable_ramp": "boolean",
            "strong_ramp": "boolean",
            "already_hot_penalty": "boolean",
            "crowded_entry": "boolean",
            "late_stage_ramp": "boolean",
            "price_data_resolution": "varchar",
            "time_to_3pp_seconds": "double",
            "max_move_10m": "double",
            "reversal_30m": "double",
            "execution_cost": "double",
            "net_close_delta": "double",
            "net_max_favorable_delta": "double",
            "net_max_adverse_delta": "double",
            "paper_trade_positive": "boolean",
            "paper_trade_strong": "boolean",
            "risk_tags": "json",
        },
    )
    _ensure_columns(
        con,
        "event_account_metrics",
        {
            "ramp_hit_rate": "double",
            "strong_ramp_rate": "double",
            "avg_entry_delay_seconds": "double",
            "avg_max_favorable_delta": "double",
            "avg_max_adverse_delta": "double",
            "already_hot_rate": "double",
            "tradable_score": "double",
            "micro_hit_rate": "double",
            "median_time_to_price_in": "double",
            "sub_10m_hit_rate": "double",
            "late_after_price_move_rate": "double",
        },
    )
    _ensure_columns(
        con,
        "live_burst_runs",
        {
            "updated_at": "timestamp",
        },
    )


def save_raw_payload(namespace: str, label: str, payload: object, fetched_at: Optional[str] = None) -> Path:
    fetched_at = fetched_at or utc_now_iso()
    safe_ts = fetched_at.replace(":", "").replace("+", "Z")
    root = SIGNAL_RAW_ROOT / namespace / label
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{safe_ts}_{stable_hash(payload)[:12]}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    return path


def api_calls_today(con: duckdb.DuckDBPyConnection, service: str = "x") -> int:
    row = con.execute(
        """
        select coalesce(sum(call_count), 0)
        from api_usage_log
        where service = ? and called_at >= current_date
        """,
        [service],
    ).fetchone()
    return int(row[0] if row else 0)


def record_api_call(
    con: duckdb.DuckDBPyConnection,
    service: str,
    endpoint: str,
    call_count: int = 1,
    notes: Optional[str] = None,
    called_at: Optional[str] = None,
) -> None:
    con.execute(
        """
        insert into api_usage_log (service, endpoint, called_at, call_count, notes)
        values (?, ?, ?, ?, ?)
        """,
        [service, endpoint, called_at or utc_now_iso(), int(call_count), notes],
    )


def upsert_markets(con: duckdb.DuckDBPyConnection, markets: Iterable[MarketRecord], updated_at: Optional[str] = None) -> int:
    updated_at = updated_at or utc_now_iso()
    rows = [
        (
            market.market_slug,
            market.event_slug,
            market.question,
            market.category,
            json.dumps(market.tags, ensure_ascii=False),
            market.end_time,
            market.resolution_source,
            json.dumps(market.clob_token_ids, ensure_ascii=False),
            market.liquidity,
            updated_at,
            json.dumps(market.raw, ensure_ascii=False, sort_keys=True),
        )
        for market in markets
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into markets
        (market_slug, event_slug, question, category, tags, end_time, resolution_source,
         clob_token_ids, liquidity, updated_at, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def insert_market_tick(
    con: duckdb.DuckDBPyConnection,
    observed_at: str,
    market_slug: Optional[str],
    token_id: Optional[str],
    best_bid: Optional[float],
    best_ask: Optional[float],
    last_trade_price: Optional[float],
    liquidity: Optional[float],
    raw: object,
    tick_source: str = "live",
) -> None:
    mid = None
    spread = None
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
    con.execute(
        """
        insert into market_ticks
        (observed_at, market_slug, token_id, best_bid, best_ask, mid, spread, last_trade_price,
         liquidity, tick_source, ingested_at, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            observed_at,
            market_slug,
            token_id,
            best_bid,
            best_ask,
            mid,
            spread,
            last_trade_price,
            liquidity,
            tick_source,
            utc_now_iso(),
            json.dumps(raw, ensure_ascii=False),
        ],
    )


def insert_market_midpoint_tick(
    con: duckdb.DuckDBPyConnection,
    observed_at: str,
    market_slug: Optional[str],
    token_id: Optional[str],
    mid: Optional[float],
    liquidity: Optional[float],
    raw: object,
) -> None:
    con.execute(
        """
        insert into market_ticks
        (observed_at, market_slug, token_id, best_bid, best_ask, mid, spread, last_trade_price,
         liquidity, tick_source, ingested_at, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [observed_at, market_slug, token_id, None, None, mid, None, None, liquidity, "live", utc_now_iso(), json.dumps(raw, ensure_ascii=False)],
    )


def insert_market_midpoint_tick_with_source(
    con: duckdb.DuckDBPyConnection,
    observed_at: str,
    market_slug: Optional[str],
    token_id: Optional[str],
    mid: Optional[float],
    liquidity: Optional[float],
    raw: object,
    tick_source: str = "live_burst",
) -> None:
    con.execute(
        """
        insert into market_ticks
        (observed_at, market_slug, token_id, best_bid, best_ask, mid, spread, last_trade_price,
         liquidity, tick_source, ingested_at, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [observed_at, market_slug, token_id, None, None, mid, None, None, liquidity, tick_source, utc_now_iso(), json.dumps(raw, ensure_ascii=False)],
    )


def insert_historical_price_ticks(con: duckdb.DuckDBPyConnection, rows_in: Iterable[dict]) -> int:
    rows = [
        (
            row["observed_at"],
            row.get("market_slug"),
            row.get("token_id"),
            _float_or_none(row.get("mid")),
            _float_or_none(row.get("liquidity")),
            row.get("tick_source") or "historical",
            row.get("ingested_at") or utc_now_iso(),
            json.dumps(row.get("raw") or row, ensure_ascii=False, sort_keys=True),
        )
        for row in rows_in
        if row.get("observed_at") and row.get("token_id") and row.get("mid") is not None
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert into market_ticks
        (observed_at, market_slug, token_id, best_bid, best_ask, mid, spread, last_trade_price,
         liquidity, tick_source, ingested_at, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(observed_at, market_slug, token_id, None, None, mid, None, None, liquidity, tick_source, ingested_at, raw_json)
         for observed_at, market_slug, token_id, mid, liquidity, tick_source, ingested_at, raw_json in rows],
    )
    return len(rows)


def replace_wallet_activity(con: duckdb.DuckDBPyConnection, wallet: str, rows: Iterable[dict], fetched_at: Optional[str] = None) -> int:
    from .utils import first_float, first_text, parse_timestamp

    fetched_at = fetched_at or utc_now_iso()
    con.execute("delete from wallet_activity where wallet = ?", [wallet])
    values = []
    for item in rows:
        price = first_float(item, "price", "avgPrice", "averagePrice")
        size = first_float(item, "size", "shares", "quantity", "amount")
        values.append(
            (
                wallet,
                first_text(item, "eventSlug", "event_slug", "slug", "marketSlug"),
                first_text(item, "conditionId", "condition_id", "marketId", "market_id", "asset"),
                first_text(item, "side", "type", "outcome"),
                price,
                size,
                price * size if price is not None and size is not None else None,
                parse_timestamp(first_text(item, "timestamp", "createdAt", "created_at", "time")),
                first_text(item, "transactionHash", "txHash", "hash", "tx"),
                fetched_at,
                json.dumps(item, ensure_ascii=False, sort_keys=True),
            )
        )
    if values:
        con.executemany(
            """
            insert into wallet_activity
            (wallet, market_slug, market_id, side, price, size, notional, activity_ts, tx_hash, fetched_at, raw_json)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
    return len(values)


def upsert_social_posts(con: duckdb.DuckDBPyConnection, posts: Iterable[SocialPost]) -> int:
    rows = [
        (
            post.platform,
            post.handle,
            post.post_id,
            post.created_at,
            post.text,
            post.url,
            stable_hash(post.raw or {"text": post.text, "id": post.post_id}),
            json.dumps(post.raw, ensure_ascii=False, sort_keys=True),
        )
        for post in posts
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into social_posts
        (platform, handle, post_id, created_at, text, url, raw_json_hash, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_x_accounts(con: duckdb.DuckDBPyConnection, accounts: Iterable[dict], updated_at: Optional[str] = None) -> int:
    updated_at = updated_at or utc_now_iso()
    rows = [
        (
            str(account["handle"]).lstrip("@"),
            account.get("user_id"),
            account.get("language"),
            account.get("role"),
            account.get("region"),
            account.get("priority"),
            _int_or_none(account.get("followers")),
            _int_or_none(account.get("following")),
            _bool_or_none(account.get("verified")),
            json.dumps(account.get("profile_metrics") or {}, ensure_ascii=False, sort_keys=True),
            account.get("status", "active"),
            account.get("notes"),
            updated_at,
        )
        for account in accounts
        if account.get("handle")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into x_accounts
        (handle, user_id, language, role, region, priority, followers, following, verified,
         profile_metrics, status, notes, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_x_posts(con: duckdb.DuckDBPyConnection, posts: Iterable[dict]) -> int:
    rows = [
        (
            str(post["post_id"]),
            str(post["handle"]).lstrip("@"),
            post["created_at"],
            post.get("text", ""),
            json.dumps(post.get("public_metrics") or {}, ensure_ascii=False, sort_keys=True),
            json.dumps(post.get("referenced_tweets") or [], ensure_ascii=False, sort_keys=True),
            post.get("lang"),
            stable_hash(post.get("raw_json") or post),
            json.dumps(post.get("raw_json") or post, ensure_ascii=False, sort_keys=True),
        )
        for post in posts
        if post.get("post_id") and post.get("handle") and post.get("created_at")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into x_posts
        (post_id, handle, created_at, text, public_metrics, referenced_tweets, lang, raw_json_hash, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_x_follow_graph(con: duckdb.DuckDBPyConnection, edges: Iterable[dict], collected_at: Optional[str] = None) -> int:
    collected_at = collected_at or utc_now_iso()
    rows = [
        (
            str(edge["source_handle"]).lstrip("@"),
            str(edge["target_handle"]).lstrip("@"),
            str(edge.get("relationship") or "following"),
            collected_at,
        )
        for edge in edges
        if edge.get("source_handle") and edge.get("target_handle")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into x_follow_graph
        (source_handle, target_handle, relationship, collected_at)
        values (?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_signal_discovery_sources(
    con: duckdb.DuckDBPyConnection,
    rows_in: Iterable[dict],
    created_at: Optional[str] = None,
) -> int:
    created_at = created_at or utc_now_iso()
    rows = [
        (
            str(row.get("run_id") or stable_hash([row.get("platform"), row.get("handle"), row.get("market_slug"), created_at])[:16]),
            str(row.get("platform") or "x"),
            str(row.get("handle") or "").lstrip("@"),
            str(row.get("market_slug") or ""),
            row.get("first_seen_at"),
            _int_or_none(row.get("post_count")) or 0,
            _float_or_none(row.get("engagement_score")) or 0.0,
            _float_or_none(row.get("discovery_score")) or 0.0,
            row.get("recommended_status") or "watch",
            json.dumps(row.get("evidence") or {}, ensure_ascii=False, sort_keys=True),
            created_at,
        )
        for row in rows_in
        if row.get("handle") and row.get("market_slug")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into signal_discovery_sources
        (run_id, platform, handle, market_slug, first_seen_at, post_count,
         engagement_score, discovery_score, recommended_status, evidence, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_account_narrative_mentions(con: duckdb.DuckDBPyConnection, rows_in: Iterable[dict]) -> int:
    rows = [
        (
            row["account"],
            row["narrative_key"],
            row.get("first_seen_at"),
            int(row.get("post_count") or 0),
            json.dumps(row.get("matched_markets") or [], ensure_ascii=False),
        )
        for row in rows_in
        if row.get("account") and row.get("narrative_key")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into account_narrative_mentions
        (account, narrative_key, first_seen_at, post_count, matched_markets)
        values (?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_account_impact_metrics(con: duckdb.DuckDBPyConnection, metrics: Iterable[dict], evaluated_at: Optional[str] = None) -> int:
    evaluated_at = evaluated_at or utc_now_iso()
    rows = [
        (
            row["account"],
            row["lookback"],
            float(row.get("speed_score") or 0),
            float(row.get("frequency_score") or 0),
            float(row.get("cascade_score") or 0),
            float(row.get("market_impact_score") or 0),
            float(row.get("source_chain_score") or 0),
            float(row.get("false_fomo_rate") or 0),
            float(row.get("final_score") or 0),
            row.get("recommended_status"),
            int(row.get("sample_size") or 0),
            float(row.get("market_link_coverage") or 0),
            _float_or_none(row.get("hit_rate_6h")),
            _float_or_none(row.get("hit_rate_24h")),
            _float_or_none(row.get("hit_rate_72h")),
            _float_or_none(row.get("avg_favorable_move")),
            _float_or_none(row.get("avg_adverse_move")),
            evaluated_at,
        )
        for row in metrics
        if row.get("account") and row.get("lookback")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into account_impact_metrics
        (account, lookback, speed_score, frequency_score, cascade_score, market_impact_score,
         source_chain_score, false_fomo_rate, final_score, recommended_status, sample_size,
         market_link_coverage, hit_rate_6h, hit_rate_24h, hit_rate_72h, avg_favorable_move,
         avg_adverse_move, evaluated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_account_source_chains(con: duckdb.DuckDBPyConnection, chains: Iterable[dict]) -> int:
    rows = [
        (
            row["downstream_account"],
            row["upstream_account"],
            row.get("evidence_type", "same_narrative_lead"),
            float(row.get("lead_time_minutes") or 0),
            json.dumps(row.get("shared_narratives") or [], ensure_ascii=False),
            float(row.get("confidence") or 0),
        )
        for row in chains
        if row.get("downstream_account") and row.get("upstream_account")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into account_source_chains
        (downstream_account, upstream_account, evidence_type, lead_time_minutes, shared_narratives, confidence)
        values (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_account_market_mentions(con: duckdb.DuckDBPyConnection, mentions: Iterable[dict]) -> int:
    rows = [
        (
            row["account"],
            row["post_id"],
            row["market_slug"],
            row["narrative_key"],
            row.get("entity") or "",
            float(row.get("confidence") or 0),
            row.get("direction"),
            row.get("post_created_at"),
        )
        for row in mentions
        if row.get("account") and row.get("post_id") and row.get("market_slug") and row.get("narrative_key")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into account_market_mentions
        (account, post_id, market_slug, narrative_key, entity, confidence, direction, post_created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_account_market_outcomes(con: duckdb.DuckDBPyConnection, outcomes: Iterable[dict], evaluated_at: Optional[str] = None) -> int:
    evaluated_at = evaluated_at or utc_now_iso()
    rows = [
        (
            row["account"],
            row["post_id"],
            row["market_slug"],
            row["horizon"],
            _float_or_none(row.get("entry_mid")),
            _float_or_none(row.get("future_mid")),
            _float_or_none(row.get("delta")),
            _float_or_none(row.get("max_favorable_delta")),
            _float_or_none(row.get("max_adverse_delta")),
            bool(row.get("is_positive")),
            evaluated_at,
        )
        for row in outcomes
        if row.get("account") and row.get("post_id") and row.get("market_slug") and row.get("horizon")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into account_market_outcomes
        (account, post_id, market_slug, horizon, entry_mid, future_mid, delta,
         max_favorable_delta, max_adverse_delta, is_positive, evaluated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_mentions(con: duckdb.DuckDBPyConnection, mentions: Iterable[EventMention], created_at: Optional[str] = None) -> int:
    created_at = created_at or utc_now_iso()
    rows = [
        (
            mention.post_id,
            mention.market_slug,
            mention.event_slug,
            json.dumps(mention.entities, ensure_ascii=False),
            json.dumps(mention.keywords, ensure_ascii=False),
            mention.confidence,
            created_at,
        )
        for mention in mentions
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into event_mentions
        (post_id, market_slug, event_slug, entities, keywords, confidence, created_at)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_narrative_snapshots(con: duckdb.DuckDBPyConnection, snapshots: Iterable[dict]) -> int:
    rows = [
        (
            snapshot["snapshot_at"],
            snapshot.get("event_family"),
            snapshot["market_slug"],
            snapshot["window"],
            int(snapshot.get("post_count") or 0),
            float(snapshot.get("weighted_post_count") or 0),
            int(snapshot.get("unique_handles") or 0),
            json.dumps(snapshot.get("source_categories") or [], ensure_ascii=False),
            json.dumps(snapshot.get("top_keywords") or [], ensure_ascii=False),
            snapshot.get("direction"),
            float(snapshot.get("sentiment_strength") or 0),
        )
        for snapshot in snapshots
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into narrative_snapshots
        (snapshot_at, event_family, market_slug, window_label, post_count, weighted_post_count,
         unique_handles, source_categories, top_keywords, direction, sentiment_strength)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_market_fomo_states(con: duckdb.DuckDBPyConnection, states: Iterable[dict]) -> int:
    rows = [
        (
            state["snapshot_at"],
            state["market_slug"],
            state.get("mid"),
            state.get("spread"),
            state.get("liquidity"),
            state.get("price_band"),
            state.get("move_1h"),
            state.get("move_6h"),
            state.get("move_24h"),
            state.get("deadline_days"),
            state.get("fomo_capacity"),
        )
        for state in states
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into market_fomo_state
        (snapshot_at, market_slug, mid, spread, liquidity, price_band, move_1h,
         move_6h, move_24h, deadline_days, fomo_capacity)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_signal_events(con: duckdb.DuckDBPyConnection, signals: Iterable[SignalScore], generated_at: Optional[str] = None) -> int:
    generated_at = generated_at or utc_now_iso()
    rows = [
        (
            signal.signal_id,
            generated_at,
            signal.event_family,
            signal.market_slug,
            signal.direction_hint,
            signal.score,
            signal.confidence,
            json.dumps(signal.evidence, ensure_ascii=False, sort_keys=True),
            json.dumps(signal.risk_tags, ensure_ascii=False),
            json.dumps(signal.source_posts, ensure_ascii=False, sort_keys=True),
            json.dumps(signal.wallet_flows, ensure_ascii=False, sort_keys=True),
            json.dumps(signal.price_window, ensure_ascii=False, sort_keys=True),
        )
        for signal in signals
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into signal_events
        (signal_id, generated_at, event_family, market_slug, direction_hint, score, confidence,
         evidence, risk_tags, source_posts, wallet_flows, price_window)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_signal_outcomes(con: duckdb.DuckDBPyConnection, outcomes: Iterable[dict]) -> int:
    rows = [
        (
            outcome["signal_id"],
            outcome["horizon"],
            outcome.get("entry_mid"),
            outcome.get("future_mid"),
            outcome.get("delta"),
            outcome.get("max_favorable_delta"),
            outcome.get("max_adverse_delta"),
            bool(outcome.get("overshoot")),
            outcome["evaluated_at"],
        )
        for outcome in outcomes
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into signal_outcomes
        (signal_id, horizon, entry_mid, future_mid, delta, max_favorable_delta,
         max_adverse_delta, overshoot, evaluated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_event_cases(con: duckdb.DuckDBPyConnection, cases: Iterable[dict], updated_at: Optional[str] = None) -> int:
    now = updated_at or utc_now_iso()
    rows = [
        (
            row["case_id"],
            row["query"],
            row.get("market_slug"),
            row.get("start_at"),
            row.get("end_at"),
            json.dumps(row.get("keywords") or [], ensure_ascii=False),
            row.get("status") or "active",
            row.get("created_at") or now,
            now,
        )
        for row in cases
        if row.get("case_id") and row.get("query")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into event_cases
        (case_id, query, market_slug, start_at, end_at, keywords, status, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_event_case_posts(con: duckdb.DuckDBPyConnection, posts: Iterable[dict]) -> int:
    rows = [
        (
            post["case_id"],
            str(post["post_id"]),
            str(post["handle"]).lstrip("@"),
            post["created_at"],
            post.get("text", ""),
            post.get("direction", "watch_only"),
            json.dumps(post.get("matched_keywords") or [], ensure_ascii=False),
            stable_hash(post.get("raw_json") or post),
            json.dumps(post.get("raw_json") or post, ensure_ascii=False, sort_keys=True),
        )
        for post in posts
        if post.get("case_id") and post.get("post_id") and post.get("handle") and post.get("created_at")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into event_case_posts
        (case_id, post_id, handle, created_at, text, direction, matched_keywords, raw_json_hash, raw_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_event_post_impacts(con: duckdb.DuckDBPyConnection, impacts: Iterable[dict], evaluated_at: Optional[str] = None) -> int:
    evaluated_at = evaluated_at or utc_now_iso()
    rows = [
        (
            row["case_id"],
            str(row["post_id"]),
            str(row["handle"]).lstrip("@"),
            row["market_slug"],
            row["horizon"],
            row.get("mode") or "event",
            _float_or_none(row.get("entry_mid")),
            _float_or_none(row.get("future_mid")),
            _float_or_none(row.get("delta")),
            _float_or_none(row.get("close_delta")),
            _float_or_none(row.get("max_favorable_delta")),
            _float_or_none(row.get("max_adverse_delta")),
            _float_or_none(row.get("entry_delay_seconds")),
            row.get("price_in_time"),
            _float_or_none(row.get("ramp_duration_minutes")),
            bool(row.get("price_move_started_before_post")),
            bool(row.get("is_positive")),
            bool(row.get("is_strong")),
            bool(row.get("tradable_ramp")),
            bool(row.get("strong_ramp")),
            bool(row.get("already_hot_penalty")),
            bool(row.get("crowded_entry")),
            bool(row.get("late_stage_ramp")),
            row.get("price_data_resolution"),
            _float_or_none(row.get("time_to_3pp_seconds")),
            _float_or_none(row.get("max_move_10m")),
            _float_or_none(row.get("reversal_30m")),
            _float_or_none(row.get("execution_cost")),
            _float_or_none(row.get("net_close_delta")),
            _float_or_none(row.get("net_max_favorable_delta")),
            _float_or_none(row.get("net_max_adverse_delta")),
            bool(row.get("paper_trade_positive")),
            bool(row.get("paper_trade_strong")),
            json.dumps(row.get("risk_tags") or [], ensure_ascii=False),
            evaluated_at,
        )
        for row in impacts
        if row.get("case_id") and row.get("post_id") and row.get("handle") and row.get("market_slug") and row.get("horizon")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into event_post_impacts
        (case_id, post_id, handle, market_slug, horizon, mode, entry_mid, future_mid, delta,
         close_delta, max_favorable_delta, max_adverse_delta, entry_delay_seconds,
         price_in_time, ramp_duration_minutes, price_move_started_before_post,
         is_positive, is_strong, tradable_ramp, strong_ramp, already_hot_penalty,
         crowded_entry, late_stage_ramp, price_data_resolution, time_to_3pp_seconds,
         max_move_10m, reversal_30m, execution_cost, net_close_delta,
         net_max_favorable_delta, net_max_adverse_delta, paper_trade_positive,
         paper_trade_strong, risk_tags, evaluated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_event_account_metrics(con: duckdb.DuckDBPyConnection, metrics: Iterable[dict], evaluated_at: Optional[str] = None) -> int:
    evaluated_at = evaluated_at or utc_now_iso()
    rows = [
        (
            row["account"],
            row["case_id"],
            float(row.get("lead_score") or 0),
            float(row.get("impact_score") or 0),
            _float_or_none(row.get("hit_rate")),
            _float_or_none(row.get("false_fomo_rate")),
            int(row.get("sample_size") or 0),
            _float_or_none(row.get("ramp_hit_rate")),
            _float_or_none(row.get("strong_ramp_rate")),
            _float_or_none(row.get("avg_entry_delay_seconds")),
            _float_or_none(row.get("avg_max_favorable_delta")),
            _float_or_none(row.get("avg_max_adverse_delta")),
            _float_or_none(row.get("already_hot_rate")),
            _float_or_none(row.get("tradable_score")),
            _float_or_none(row.get("micro_hit_rate")),
            _float_or_none(row.get("median_time_to_price_in")),
            _float_or_none(row.get("sub_10m_hit_rate")),
            _float_or_none(row.get("late_after_price_move_rate")),
            row.get("recommended_status"),
            evaluated_at,
        )
        for row in metrics
        if row.get("account") and row.get("case_id")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into event_account_metrics
        (account, case_id, lead_score, impact_score, hit_rate, false_fomo_rate,
         sample_size, ramp_hit_rate, strong_ramp_rate, avg_entry_delay_seconds,
         avg_max_favorable_delta, avg_max_adverse_delta, already_hot_rate,
         tradable_score, micro_hit_rate, median_time_to_price_in, sub_10m_hit_rate,
         late_after_price_move_rate, recommended_status, evaluated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_post_market_semantic_matches(con: duckdb.DuckDBPyConnection, matches: Iterable[dict], created_at: Optional[str] = None) -> int:
    created_at = created_at or utc_now_iso()
    rows = [
        (
            row["case_id"],
            str(row["post_id"]),
            str(row["handle"]).lstrip("@"),
            row["market_slug"],
            row.get("method") or "semantic",
            _float_or_none(row.get("similarity")),
            json.dumps(row.get("matched_concepts") or [], ensure_ascii=False),
            json.dumps(row.get("rejected_concepts") or [], ensure_ascii=False),
            row.get("decision") or "matched",
            created_at,
        )
        for row in matches
        if row.get("case_id") and row.get("post_id") and row.get("handle") and row.get("market_slug")
    ]
    if not rows:
        return 0
    con.executemany(
        """
        insert or replace into post_market_semantic_matches
        (case_id, post_id, handle, market_slug, method, similarity, matched_concepts,
         rejected_concepts, decision, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def upsert_live_burst_run(con: duckdb.DuckDBPyConnection, row: dict, updated_at: Optional[str] = None) -> None:
    updated_at = updated_at or utc_now_iso()
    run_id = row.get("run_id") or stable_hash([row.get("case_id"), row.get("post_id")])[:24]
    con.execute("delete from live_burst_runs where run_id = ? or (case_id = ? and post_id = ?)", [run_id, row["case_id"], str(row["post_id"])])
    con.execute(
        """
        insert into live_burst_runs
        (run_id, case_id, post_id, handle, triggered_at, confidence, status,
         planned_calls, ticks_written, error, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            row["case_id"],
            str(row["post_id"]),
            row.get("handle"),
            row.get("triggered_at") or updated_at,
            _float_or_none(row.get("confidence")),
            row.get("status") or "planned",
            _int_or_none(row.get("planned_calls")) or 0,
            _int_or_none(row.get("ticks_written")) or 0,
            row.get("error"),
            updated_at,
        ],
    )


def live_burst_run_exists(con: duckdb.DuckDBPyConnection, case_id: str, post_id: str) -> bool:
    row = con.execute(
        "select 1 from live_burst_runs where case_id = ? and post_id = ? limit 1",
        [case_id, str(post_id)],
    ).fetchone()
    return bool(row)


def live_burst_trigger_candidates(
    con: duckdb.DuckDBPyConnection,
    case_ids: Sequence[str],
    min_confidence: float,
    limit: int = 1,
    min_match_created_at: Optional[str] = None,
    min_post_created_at: Optional[str] = None,
) -> List[dict]:
    if not case_ids:
        return []
    placeholders = ",".join(["?"] * len(case_ids))
    rows = con.execute(
        f"""
        select m.case_id, m.post_id, m.handle, m.market_slug, m.similarity, m.created_at,
               p.created_at as post_created_at, p.direction, p.text
        from post_market_semantic_matches m
        left join event_case_posts p on p.case_id = m.case_id and p.post_id = m.post_id
        where m.method = 'cloud'
          and coalesce(m.similarity, 0) >= ?
          and (? is null or m.created_at >= ?)
          and (? is null or p.created_at >= ?)
          and m.case_id in ({placeholders})
          and not exists (
              select 1 from live_burst_runs r
              where r.case_id = m.case_id and r.post_id = m.post_id
          )
        order by m.similarity desc, coalesce(p.created_at, m.created_at) asc
        limit ?
        """,
        [
            float(min_confidence),
            min_match_created_at,
            min_match_created_at,
            min_post_created_at,
            min_post_created_at,
            *case_ids,
            int(limit),
        ],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def live_burst_runs_for_case(con: duckdb.DuckDBPyConnection, case_id: str) -> List[dict]:
    rows = con.execute(
        """
        select run_id, case_id, post_id, handle, triggered_at, confidence, status,
               planned_calls, ticks_written, error, updated_at
        from live_burst_runs
        where case_id = ?
        order by triggered_at desc
        """,
        [case_id],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def record_telegram_alert(con: duckdb.DuckDBPyConnection, signal_id: str, payload: str, status: str, error: Optional[str] = None) -> None:
    con.execute(
        """
        insert into telegram_alerts (signal_id, sent_at, status, payload, error)
        values (?, ?, ?, ?, ?)
        """,
        [signal_id, utc_now_iso(), status, payload, error],
    )


def recent_signals(con: duckdb.DuckDBPyConnection, since_iso: str, min_score: int = 0) -> List[dict]:
    rows = con.execute(
        """
        select signal_id, generated_at, event_family, market_slug, direction_hint, score,
               confidence, evidence, risk_tags, source_posts, wallet_flows, price_window
        from signal_events
        where generated_at >= ? and score >= ?
        order by score desc, generated_at desc
        """,
        [since_iso, min_score],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def social_posts_since(con: duckdb.DuckDBPyConnection, since_iso: str) -> List[dict]:
    rows = con.execute(
        """
        select platform, handle, post_id, created_at, text, url, raw_json
        from social_posts
        where created_at >= ?
        order by created_at desc
        """,
        [since_iso],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def active_markets(con: duckdb.DuckDBPyConnection) -> List[dict]:
    rows = con.execute(
        """
        select market_slug, event_slug, question, category, tags, end_time, resolution_source,
               clob_token_ids, liquidity, raw_json
        from markets
        order by updated_at desc
        """
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def latest_tick(con: duckdb.DuckDBPyConnection, market_slug: str) -> Optional[dict]:
    row = con.execute(
        """
        select observed_at, market_slug, token_id, best_bid, best_ask, mid, spread,
               last_trade_price, liquidity
        from market_ticks
        where market_slug = ?
        order by observed_at desc
        limit 1
        """,
        [market_slug],
    ).fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in con.description]
    return dict(zip(columns, row))


def wallet_flows_for_market(con: duckdb.DuckDBPyConnection, market_slug: str, start_iso: str, end_iso: str) -> List[dict]:
    rows = con.execute(
        """
        select wallet, market_slug, market_id, side, price, size, notional, activity_ts, tx_hash
        from wallet_activity
        where market_slug = ? and activity_ts between ? and ?
        order by abs(coalesce(notional, 0)) desc
        limit 20
        """,
        [market_slug, start_iso, end_iso],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def alerted_signal_ids(con: duckdb.DuckDBPyConnection) -> set:
    rows = con.execute("select distinct signal_id from telegram_alerts where status = 'sent'").fetchall()
    return {row[0] for row in rows}


def _int_or_none(value: object) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: object) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _ensure_columns(con: duckdb.DuckDBPyConnection, table: str, columns: dict) -> None:
    existing = {
        str(row[1])
        for row in con.execute(f"pragma table_info('{table}')").fetchall()
    }
    for name, ddl in columns.items():
        if name not in existing:
            con.execute(f"alter table {table} add column {name} {ddl}")
