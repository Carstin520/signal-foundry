import json
import logging
from pathlib import Path

import duckdb

from quant_sol.capture.hip4_l2 import (
    CaptureConfig,
    BackoffPolicy,
    GapTracker,
    Hip4CaptureRunner,
    Hip4Market,
    PartitionedParquetWriter,
    SNAPSHOT_COLUMNS,
    TRADE_COLUMNS,
    load_capture_config,
    normalize_l2_book,
    normalize_trade,
)
from scripts.check_l2_integrity import scan_integrity


def test_hip4_schema_normalization_matches_v2_plan() -> None:
    market = Hip4Market(coin="#650", market_uid="hip4:#650")
    markets = {"#650": market}

    snapshot_rows = normalize_l2_book(
        {
            "coin": "#650",
            "time": 1_779_120_000_123,
            "levels": [
                [{"px": "0.59961", "sz": "214.0", "n": 2}],
                [{"px": "0.6085", "sz": "27.0", "n": 1}],
            ],
        },
        markets,
        max_levels=10,
    )
    trade_row = normalize_trade(
        {"coin": "#650", "side": "B", "px": "0.601", "sz": "3.5", "time": 1_779_120_000_456, "tid": 123},
        markets,
    )

    assert list(snapshot_rows[0].keys()) == SNAPSHOT_COLUMNS
    assert list(trade_row.keys()) == TRADE_COLUMNS
    assert snapshot_rows[0] == {
        "ts_ns": 1_779_120_000_123_000_000,
        "venue": "hip4",
        "market_uid": "hip4:#650",
        "seq": 1_779_120_000_123,
        "side": "bid",
        "level": 0,
        "px": 0.59961,
        "sz": 214.0,
    }
    assert snapshot_rows[1]["side"] == "ask"
    assert trade_row["side"] == "buy"
    assert trade_row["taker_id_hash"] is None


def test_gap_tracker_warns_on_gap_above_threshold(caplog) -> None:
    tracker = GapTracker(gap_warn_seconds=5, logger=logging.getLogger("test.hip4.gap"))

    with caplog.at_level(logging.WARNING):
        assert tracker.observe("l2_snapshots", "hip4:#650", 1_000) is None
        gap = tracker.observe("l2_snapshots", "hip4:#650", 6_500)

    assert gap is not None
    assert gap.gap_seconds == 5.5
    assert "HIP-4 l2_snapshots gap 5.500s for hip4:#650" in caplog.text


def test_reconnect_uses_backoff_and_resubscribes_with_mock_ws(tmp_path) -> None:
    first = FakeWebSocket(
        [
            {
                "channel": "l2Book",
                "data": {
                    "coin": "#650",
                    "time": 1_779_148_800_000,
                    "levels": [
                        [{"px": "0.4", "sz": "10"}],
                        [{"px": "0.41", "sz": "12"}],
                    ],
                },
            }
        ],
        error=RuntimeError("first disconnect"),
    )
    second = FakeWebSocket(
        [
            {
                "channel": "trades",
                "data": [{"coin": "#650", "side": "A", "px": "0.41", "sz": "2", "time": 1_779_148_801_000, "tid": 7}],
            }
        ],
        error=RuntimeError("second disconnect"),
    )
    sockets = [first, second]
    sleep_calls = []
    config = _test_config(tmp_path, flush_rows=1)
    runner = Hip4CaptureRunner(
        config,
        ws_factory=lambda _url: sockets.pop(0),
        sleep_fn=sleep_calls.append,
    )

    runner.run(max_reconnects=1)

    assert sleep_calls == [1.0]
    assert _subscription_types(first) == ["l2Book", "trades"]
    assert _subscription_types(second) == ["l2Book", "trades"]
    assert list((tmp_path / "snapshots" / "venue=hip4" / "date=2026-05-19").glob("*.parquet"))
    assert list((tmp_path / "trades" / "venue=hip4" / "date=2026-05-19").glob("*.parquet"))


def test_partitioned_parquet_layout_and_integrity_scan(tmp_path) -> None:
    writer = PartitionedParquetWriter(
        snapshot_root=tmp_path / "l2_snapshots",
        trade_root=tmp_path / "l2_trades",
        flush_rows=100,
        flush_interval_seconds=60,
    )
    writer.write_snapshots(
        [
            {
                "ts_ns": 1_779_148_800_000_000_000,
                "venue": "hip4",
                "market_uid": "hip4:#650",
                "seq": 1_779_148_800_000,
                "side": "bid",
                "level": 0,
                "px": 0.5,
                "sz": 10.0,
            },
            {
                "ts_ns": 1_779_148_806_250_000_000,
                "venue": "hip4",
                "market_uid": "hip4:#650",
                "seq": 1_779_148_806_250,
                "side": "bid",
                "level": 0,
                "px": 0.51,
                "sz": 11.0,
            },
        ]
    )
    writer.write_trades(
        [
            {
                "ts_ns": 1_779_148_801_000_000_000,
                "venue": "hip4",
                "market_uid": "hip4:#650",
                "seq": 1_779_148_801_000,
                "side": "buy",
                "px": 0.505,
                "sz": 1.0,
                "taker_id_hash": None,
            }
        ]
    )
    writer.flush()

    snapshot_files = list((tmp_path / "l2_snapshots" / "venue=hip4" / "date=2026-05-19").glob("*.parquet"))
    trade_files = list((tmp_path / "l2_trades" / "venue=hip4" / "date=2026-05-19").glob("*.parquet"))
    assert snapshot_files
    assert trade_files

    con = duckdb.connect(":memory:")
    try:
        snapshot_columns = [
            row[0]
            for row in con.execute(f"select * from read_parquet('{snapshot_files[0]}', hive_partitioning=false) limit 0").description
        ]
        trade_columns = [
            row[0]
            for row in con.execute(f"select * from read_parquet('{trade_files[0]}', hive_partitioning=false) limit 0").description
        ]
    finally:
        con.close()

    assert snapshot_columns == SNAPSHOT_COLUMNS
    assert trade_columns == TRADE_COLUMNS

    stats = scan_integrity(
        snapshot_root=tmp_path / "l2_snapshots",
        trade_root=tmp_path / "l2_trades",
        date="2026-05-19",
        gap_threshold_seconds=5,
    )
    snapshot_stats = [row for row in stats if row.stream == "l2_snapshots"][0]
    assert snapshot_stats.rows == 2
    assert snapshot_stats.max_gap_seconds == 6.25
    assert snapshot_stats.gaps_gt_threshold == 1


def test_load_capture_config_uses_read_only_public_defaults(tmp_path) -> None:
    path = tmp_path / "capture_hip4.yaml"
    path.write_text(
        """
venue: hip4
markets:
  - coin: "#650"
channels: [l2Book, trades]
""",
        encoding="utf-8",
    )

    config = load_capture_config(path)

    assert config.websocket_url == "wss://api.hyperliquid.xyz/ws"
    assert config.markets[0].market_uid == "hip4:#650"
    assert config.snapshot_root == Path("data/raw/l2_snapshots")
    assert config.trade_root == Path("data/raw/l2_trades")


def _test_config(tmp_path: Path, *, flush_rows: int = 1000) -> CaptureConfig:
    return CaptureConfig(
        venue="hip4",
        websocket_url="wss://example.invalid/ws",
        markets=(Hip4Market(coin="#650", market_uid="hip4:#650"),),
        channels=("l2Book", "trades"),
        snapshot_root=tmp_path / "snapshots",
        trade_root=tmp_path / "trades",
        max_book_levels=10,
        flush_rows=flush_rows,
        flush_interval_seconds=60,
        gap_warn_seconds=5,
        backoff=BackoffPolicy(initial_seconds=1, max_seconds=10, multiplier=2),
    )


def _subscription_types(ws: "FakeWebSocket") -> list:
    return [json.loads(raw)["subscription"]["type"] for raw in ws.sent]


class FakeWebSocket:
    def __init__(self, messages, *, error=None) -> None:
        self.messages = [json.dumps(message) for message in messages]
        self.error = error
        self.sent = []
        self.closed = False

    def send(self, raw: str) -> None:
        self.sent.append(raw)

    def recv(self) -> str:
        if self.messages:
            return self.messages.pop(0)
        if self.error:
            raise self.error
        raise RuntimeError("closed")

    def close(self) -> None:
        self.closed = True
