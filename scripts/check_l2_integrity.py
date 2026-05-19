#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import duckdb


DEFAULT_SNAPSHOT_ROOT = Path("data/raw/l2_snapshots")
DEFAULT_TRADE_ROOT = Path("data/raw/l2_trades")


@dataclass(frozen=True)
class IntegrityStats:
    stream: str
    date: str
    market_uid: str
    rows: int
    start_utc: str
    end_utc: str
    coverage_seconds: float
    max_gap_seconds: float
    gaps_gt_threshold: int


def latest_partition_date(roots: Iterable[Path]) -> Optional[str]:
    dates = set()
    for root in roots:
        if not root.exists():
            continue
        for path in root.glob("venue=*/date=*"):
            if path.is_dir() and path.name.startswith("date="):
                dates.add(path.name.split("=", 1)[1])
    return max(dates) if dates else None


def scan_integrity(
    *,
    snapshot_root: Path = DEFAULT_SNAPSHOT_ROOT,
    trade_root: Path = DEFAULT_TRADE_ROOT,
    date: Optional[str] = None,
    gap_threshold_seconds: float = 5.0,
) -> List[IntegrityStats]:
    scan_date = date or latest_partition_date([snapshot_root, trade_root])
    if not scan_date:
        return []

    rows: List[IntegrityStats] = []
    for stream, root in (("l2_snapshots", snapshot_root), ("l2_trades", trade_root)):
        files = sorted(root.glob(f"venue=*/date={scan_date}/*.parquet"))
        if not files:
            continue
        rows.extend(_scan_stream(stream, root, scan_date, gap_threshold_seconds))
    return rows


def format_stats(stats: Sequence[IntegrityStats]) -> str:
    if not stats:
        return "No L2 Parquet files found for the requested date."

    headers = [
        "stream",
        "date",
        "market_uid",
        "rows",
        "coverage_s",
        "max_gap_s",
        "gaps>threshold",
        "start_utc",
        "end_utc",
    ]
    lines = [" | ".join(headers), " | ".join("---" for _ in headers)]
    for row in stats:
        lines.append(
            " | ".join(
                [
                    row.stream,
                    row.date,
                    row.market_uid,
                    str(row.rows),
                    f"{row.coverage_seconds:.3f}",
                    f"{row.max_gap_seconds:.3f}",
                    str(row.gaps_gt_threshold),
                    row.start_utc,
                    row.end_utc,
                ]
            )
        )
    return "\n".join(lines)


def _scan_stream(stream: str, root: Path, date: str, gap_threshold_seconds: float) -> List[IntegrityStats]:
    pattern = str(root / "venue=*" / f"date={date}" / "*.parquet").replace("'", "''")
    con = duckdb.connect(":memory:")
    try:
        result = con.execute(
            f"""
            with ordered as (
                select
                    market_uid,
                    ts_ns,
                    seq,
                    (ts_ns - lag(ts_ns) over (
                        partition by market_uid
                        order by ts_ns, seq
                    )) / 1000000000.0 as gap_seconds
                from read_parquet('{pattern}', union_by_name=true)
            )
            select
                market_uid,
                count(*)::bigint as rows,
                min(ts_ns)::bigint as start_ts_ns,
                max(ts_ns)::bigint as end_ts_ns,
                max(coalesce(gap_seconds, 0.0))::double as max_gap_seconds,
                sum(case when gap_seconds > ? then 1 else 0 end)::bigint as gaps_gt_threshold
            from ordered
            group by market_uid
            order by market_uid
            """,
            [gap_threshold_seconds],
        ).fetchall()
    finally:
        con.close()

    stats = []
    for market_uid, rows, start_ts_ns, end_ts_ns, max_gap_seconds, gaps_gt_threshold in result:
        coverage_seconds = (int(end_ts_ns) - int(start_ts_ns)) / 1_000_000_000 if rows else 0.0
        stats.append(
            IntegrityStats(
                stream=stream,
                date=date,
                market_uid=str(market_uid),
                rows=int(rows),
                start_utc=_format_ts_ns(int(start_ts_ns)),
                end_utc=_format_ts_ns(int(end_ts_ns)),
                coverage_seconds=coverage_seconds,
                max_gap_seconds=float(max_gap_seconds or 0.0),
                gaps_gt_threshold=int(gaps_gt_threshold or 0),
            )
        )
    return stats


def _format_ts_ns(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).isoformat()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Check latest L2 Parquet capture coverage and gap statistics.")
    parser.add_argument("--snapshot-root", type=Path, default=DEFAULT_SNAPSHOT_ROOT)
    parser.add_argument("--trade-root", type=Path, default=DEFAULT_TRADE_ROOT)
    parser.add_argument("--date", help="UTC partition date YYYY-MM-DD. Defaults to latest available date.")
    parser.add_argument("--gap-threshold-seconds", type=float, default=5.0)
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of markdown table.")
    args = parser.parse_args(argv)

    stats = scan_integrity(
        snapshot_root=args.snapshot_root,
        trade_root=args.trade_root,
        date=args.date,
        gap_threshold_seconds=args.gap_threshold_seconds,
    )
    if args.json:
        print(json.dumps([asdict(row) for row in stats], indent=2))
    else:
        print(format_stats(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
