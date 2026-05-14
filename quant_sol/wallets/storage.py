import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import duckdb

from .config import DB_PATH, RAW_ROOT, REPORT_ROOT, WATCHLIST, WalletConfig


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def json_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def connect(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    ensure_schema(con)
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table if not exists wallets (
            label varchar primary key,
            address varchar,
            status varchar not null,
            notes varchar
        )
        """
    )
    con.execute(
        """
        create table if not exists raw_api_snapshots (
            wallet varchar not null,
            endpoint varchar not null,
            fetched_at timestamp not null,
            raw_path varchar not null,
            response_hash varchar not null,
            primary key (wallet, endpoint, response_hash)
        )
        """
    )
    con.execute(
        """
        create table if not exists positions (
            wallet varchar not null,
            fetched_at timestamp not null,
            event_slug varchar,
            market_id varchar,
            market varchar,
            category varchar,
            outcome varchar,
            size double,
            value double,
            pnl double,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists closed_positions (
            wallet varchar not null,
            fetched_at timestamp not null,
            event_slug varchar,
            market_id varchar,
            market varchar,
            category varchar,
            outcome varchar,
            size double,
            value double,
            pnl double,
            volume double,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists activity (
            wallet varchar not null,
            fetched_at timestamp not null,
            activity_ts timestamp,
            event_slug varchar,
            market_id varchar,
            market varchar,
            category varchar,
            side varchar,
            price double,
            size double,
            tx_hash varchar,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists wallet_values (
            wallet varchar not null,
            fetched_at timestamp not null,
            value double,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists market_metadata (
            event_slug varchar primary key,
            category varchar,
            tags json,
            fetched_at timestamp not null,
            raw_json json
        )
        """
    )
    con.execute(
        """
        create table if not exists wallet_metrics (
            wallet varchar not null,
            analyzed_at timestamp not null,
            address varchar,
            status varchar,
            realized_pnl double,
            unrealized_pnl double,
            total_pnl double,
            win_rate double,
            closed_markets integer,
            open_markets integer,
            total_volume double,
            top_category varchar,
            top_category_pnl_share double,
            max_market_pnl double,
            max_market_pnl_share double,
            late_entry_ratio double,
            confidence varchar,
            risk_tags json
        )
        """
    )
    _add_column_if_missing(con, "positions", "event_slug", "varchar")
    _add_column_if_missing(con, "closed_positions", "event_slug", "varchar")
    _add_column_if_missing(con, "activity", "event_slug", "varchar")
    _add_column_if_missing(con, "activity", "category", "varchar")


def _add_column_if_missing(con: duckdb.DuckDBPyConnection, table: str, column: str, column_type: str) -> None:
    exists = con.execute(
        """
        select count(*)
        from information_schema.columns
        where table_name = ? and column_name = ?
        """,
        [table, column],
    ).fetchone()[0]
    if not exists:
        con.execute(f"alter table {table} add column {column} {column_type}")


def seed_wallets(con: duckdb.DuckDBPyConnection, wallets: Dict[str, WalletConfig] = WATCHLIST) -> None:
    con.executemany(
        """
        insert or replace into wallets (label, address, status, notes)
        values (?, ?, ?, ?)
        """,
        [(w.label, w.address, w.status, w.notes) for w in wallets.values()],
    )


def save_payload(
    con: duckdb.DuckDBPyConnection,
    wallet: WalletConfig,
    endpoint: str,
    payload: object,
    fetched_at: Optional[str] = None,
    raw_root: Path = RAW_ROOT,
) -> Path:
    fetched_at = fetched_at or utc_now_iso()
    digest = json_hash(payload)
    existing = con.execute(
        """
        select raw_path from raw_api_snapshots
        where wallet = ? and endpoint = ? and response_hash = ?
        """,
        [wallet.label, endpoint, digest],
    ).fetchone()
    if existing:
        replace_endpoint_rows(con, wallet.label, endpoint, fetched_at, payload)
        return Path(existing[0])

    safe_ts = fetched_at.replace(":", "").replace("+", "Z")
    raw_dir = raw_root / wallet.label / endpoint
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"{safe_ts}.json"
    raw_path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")

    con.execute(
        """
        insert into raw_api_snapshots (wallet, endpoint, fetched_at, raw_path, response_hash)
        values (?, ?, ?, ?, ?)
        """,
        [wallet.label, endpoint, fetched_at, str(raw_path), digest],
    )
    replace_endpoint_rows(con, wallet.label, endpoint, fetched_at, payload)
    return raw_path


def replace_endpoint_rows(
    con: duckdb.DuckDBPyConnection,
    wallet_label: str,
    endpoint: str,
    fetched_at: str,
    payload: object,
) -> None:
    if endpoint == "positions":
        con.execute("delete from positions where wallet = ?", [wallet_label])
        rows = [_position_row(wallet_label, fetched_at, item) for item in _rows(payload)]
        if rows:
            con.executemany(
                """
            insert into positions
            (wallet, fetched_at, event_slug, market_id, market, category, outcome, size, value, pnl, raw_json)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    elif endpoint == "closed-positions":
        con.execute("delete from closed_positions where wallet = ?", [wallet_label])
        rows = [_closed_position_row(wallet_label, fetched_at, item) for item in _rows(payload)]
        if rows:
            con.executemany(
                """
            insert into closed_positions
            (wallet, fetched_at, event_slug, market_id, market, category, outcome, size, value, pnl, volume, raw_json)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    elif endpoint == "activity":
        con.execute("delete from activity where wallet = ?", [wallet_label])
        rows = [_activity_row(wallet_label, fetched_at, item) for item in _rows(payload)]
        if rows:
            con.executemany(
                """
            insert into activity
            (wallet, fetched_at, activity_ts, event_slug, market_id, market, category, side, price, size, tx_hash, raw_json)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    elif endpoint == "value":
        con.execute("delete from wallet_values where wallet = ?", [wallet_label])
        con.execute(
            """
            insert into wallet_values (wallet, fetched_at, value, raw_json)
            values (?, ?, ?, ?)
            """,
            [wallet_label, fetched_at, _first_number(payload, ["value", "portfolioValue", "totalValue"]), _json(payload)],
        )


def latest_metrics(con: duckdb.DuckDBPyConnection) -> List[dict]:
    rows = con.execute(
        """
        with ranked as (
            select *,
                   row_number() over (partition by wallet order by analyzed_at desc) as rn
            from wallet_metrics
        )
        select wallet, analyzed_at, address, status, realized_pnl, unrealized_pnl, total_pnl,
               win_rate, closed_markets, open_markets, total_volume, top_category,
               top_category_pnl_share, max_market_pnl, max_market_pnl_share, late_entry_ratio,
               confidence, risk_tags
        from ranked
        where rn = 1
        order by wallet
        """
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _rows(payload: object) -> List[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def _position_row(wallet_label: str, fetched_at: str, item: dict) -> Sequence[object]:
    return (
        wallet_label,
        fetched_at,
        _first_text(item, ["eventSlug", "event_slug"]),
        _first_text(item, ["conditionId", "condition_id", "marketId", "market_id", "asset"]),
        _first_text(item, ["title", "market", "question", "slug"]),
        _first_text(item, ["category", "eventCategory", "marketCategory"]),
        _first_text(item, ["outcome", "outcomeName", "side"]),
        _first_number(item, ["size", "shares", "quantity"]),
        _first_number(item, ["value", "currentValue", "cashValue"]),
        _first_number(item, ["pnl", "profit", "realizedPnl", "unrealizedPnl"]),
        _json(item),
    )


def _closed_position_row(wallet_label: str, fetched_at: str, item: dict) -> Sequence[object]:
    return (
        wallet_label,
        fetched_at,
        _first_text(item, ["eventSlug", "event_slug"]),
        _first_text(item, ["conditionId", "condition_id", "marketId", "market_id", "asset"]),
        _first_text(item, ["title", "market", "question", "slug"]),
        _first_text(item, ["category", "eventCategory", "marketCategory"]),
        _first_text(item, ["outcome", "outcomeName", "side"]),
        _first_number(item, ["size", "shares", "quantity"]),
        _first_number(item, ["value", "currentValue", "cashValue"]),
        _first_number(item, ["pnl", "profit", "realizedPnl", "realized_pnl"]),
        _first_number(item, ["volume", "amount", "totalAmount", "totalBought"]),
        _json(item),
    )


def _activity_row(wallet_label: str, fetched_at: str, item: dict) -> Sequence[object]:
    return (
        wallet_label,
        fetched_at,
        _parse_timestamp(_first_value(item, ["timestamp", "createdAt", "created_at", "time"])),
        _first_text(item, ["eventSlug", "event_slug"]),
        _first_text(item, ["conditionId", "condition_id", "marketId", "market_id", "asset"]),
        _first_text(item, ["title", "market", "question", "slug"]),
        _first_text(item, ["category", "eventCategory", "marketCategory"]),
        _first_text(item, ["side", "type", "outcome"]),
        _first_number(item, ["price", "avgPrice", "averagePrice"]),
        _first_number(item, ["size", "shares", "quantity", "amount"]),
        _first_text(item, ["transactionHash", "txHash", "hash", "tx"]),
        _json(item),
    )


def _first_value(item: object, keys: Iterable[str]) -> object:
    if not isinstance(item, dict):
        return None
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def _first_text(item: dict, keys: Iterable[str]) -> Optional[str]:
    value = _first_value(item, keys)
    if value is None:
        return None
    return str(value)


def _first_number(item: object, keys: Iterable[str]) -> Optional[float]:
    value = _first_value(item, keys)
    if isinstance(value, dict):
        value = _first_value(value, ["value", "amount"])
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: object) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    return str(value)


def _json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def ensure_report_root() -> Path:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    return REPORT_ROOT


def missing_event_slugs(con: duckdb.DuckDBPyConnection, wallet_label: Optional[str] = None) -> List[str]:
    params: List[object] = []
    wallet_clause = ""
    if wallet_label:
        wallet_clause = " and wallet = ?"
        params.append(wallet_label)
    rows = con.execute(
        f"""
        with slugs as (
            select event_slug from positions where event_slug is not null and event_slug <> '' {wallet_clause}
            union
            select event_slug from closed_positions where event_slug is not null and event_slug <> '' {wallet_clause}
            union
            select event_slug from activity where event_slug is not null and event_slug <> '' {wallet_clause}
        )
        select event_slug
        from slugs
        where event_slug not in (select event_slug from market_metadata)
        order by event_slug
        """,
        params * 3 if wallet_label else [],
    ).fetchall()
    return [row[0] for row in rows]


def upsert_market_metadata(
    con: duckdb.DuckDBPyConnection,
    event_slug: str,
    payload: object,
    fetched_at: Optional[str] = None,
) -> None:
    fetched_at = fetched_at or utc_now_iso()
    category, tags = _category_and_tags(payload)
    con.execute(
        """
        insert or replace into market_metadata (event_slug, category, tags, fetched_at, raw_json)
        values (?, ?, ?, ?, ?)
        """,
        [event_slug, category, json.dumps(tags, ensure_ascii=False), fetched_at, _json(payload)],
    )


def apply_market_metadata(con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute("select event_slug, category from market_metadata where category is not null").fetchall()
    for event_slug, category in rows:
        con.execute(
            "update positions set category = ? where event_slug = ? and (category is null or category = 'unknown')",
            [category, event_slug],
        )
        con.execute(
            "update closed_positions set category = ? where event_slug = ? and (category is null or category = 'unknown')",
            [category, event_slug],
        )
        con.execute(
            "update activity set category = ? where event_slug = ? and (category is null or category = 'unknown')",
            [category, event_slug],
        )


def _category_and_tags(payload: object) -> Sequence[object]:
    if not isinstance(payload, dict):
        return None, []
    tags = payload.get("tags")
    if not isinstance(tags, list):
        tags = []
    labels = [tag.get("label") for tag in tags if isinstance(tag, dict) and tag.get("label")]
    category = labels[0] if labels else None
    return category, labels
