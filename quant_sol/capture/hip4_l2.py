from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import duckdb
import pandas as pd
import typer
import yaml


LOGGER = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/capture_hip4.yaml")
DEFAULT_WS_URL = "wss://api.hyperliquid.xyz/ws"

SNAPSHOT_COLUMNS = ["ts_ns", "venue", "market_uid", "seq", "side", "level", "px", "sz"]
TRADE_COLUMNS = ["ts_ns", "venue", "market_uid", "seq", "side", "px", "sz", "taker_id_hash"]

app = typer.Typer(help="Read-only HIP-4 L2/trades capture.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """HIP-4 capture commands."""


@dataclass(frozen=True)
class Hip4Market:
    coin: str
    market_uid: str
    label: str = ""

    @classmethod
    def from_config(cls, payload: Mapping[str, Any]) -> "Hip4Market":
        coin = str(payload.get("coin") or "").strip()
        if not coin:
            raise ValueError("HIP-4 market config requires coin")
        market_uid = str(payload.get("market_uid") or f"hip4:{coin}").strip()
        label = str(payload.get("label") or "").strip()
        return cls(coin=coin, market_uid=market_uid, label=label)


@dataclass(frozen=True)
class BackoffPolicy:
    initial_seconds: float = 1.0
    max_seconds: float = 60.0
    multiplier: float = 2.0


@dataclass(frozen=True)
class CaptureConfig:
    venue: str
    websocket_url: str
    markets: Tuple[Hip4Market, ...]
    channels: Tuple[str, ...]
    snapshot_root: Path
    trade_root: Path
    max_book_levels: int
    flush_rows: int
    flush_interval_seconds: float
    gap_warn_seconds: float
    backoff: BackoffPolicy


@dataclass(frozen=True)
class GapObservation:
    stream: str
    market_uid: str
    previous_seq: int
    current_seq: int
    gap_seconds: float


class ExponentialBackoff:
    def __init__(self, policy: BackoffPolicy) -> None:
        self.policy = policy
        self._next_seconds = policy.initial_seconds

    def reset(self) -> None:
        self._next_seconds = self.policy.initial_seconds

    def next_sleep(self) -> float:
        sleep_seconds = self._next_seconds
        self._next_seconds = min(self.policy.max_seconds, self._next_seconds * self.policy.multiplier)
        return sleep_seconds


class GapTracker:
    """Tracks venue-clock gaps.

    Hyperliquid's public WsBook does not expose an incrementing book sequence in
    the official API; its venue-provided millisecond `time` field is used as the
    sequence clock for Phase 0a.
    """

    def __init__(self, *, gap_warn_seconds: float = 5.0, logger: logging.Logger = LOGGER) -> None:
        self.gap_warn_seconds = gap_warn_seconds
        self.logger = logger
        self.last_seq: Dict[Tuple[str, str], int] = {}

    def is_stale(self, stream: str, market_uid: str, seq: int) -> bool:
        previous = self.last_seq.get((stream, market_uid))
        return previous is not None and seq < previous

    def observe(self, stream: str, market_uid: str, seq: int) -> Optional[GapObservation]:
        key = (stream, market_uid)
        previous = self.last_seq.get(key)
        if previous is None:
            self.last_seq[key] = seq
            return None
        if seq < previous:
            return None
        self.last_seq[key] = seq
        if seq == previous:
            return None

        gap_seconds = (seq - previous) / 1000.0
        if gap_seconds <= self.gap_warn_seconds:
            return None

        gap = GapObservation(
            stream=stream,
            market_uid=market_uid,
            previous_seq=previous,
            current_seq=seq,
            gap_seconds=gap_seconds,
        )
        self.logger.warning(
            "HIP-4 %s gap %.3fs for %s: seq %s -> %s exceeds %.3fs",
            stream,
            gap_seconds,
            market_uid,
            previous,
            seq,
            self.gap_warn_seconds,
        )
        return gap

    def state_summary(self) -> str:
        if not self.last_seq:
            return "none"
        items = [f"{stream}:{market_uid}={seq}" for (stream, market_uid), seq in sorted(self.last_seq.items())]
        return ", ".join(items[:12]) + (" ..." if len(items) > 12 else "")


class PartitionedParquetWriter:
    def __init__(
        self,
        *,
        snapshot_root: Path,
        trade_root: Path,
        flush_rows: int = 1000,
        flush_interval_seconds: float = 5.0,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.snapshot_root = snapshot_root
        self.trade_root = trade_root
        self.flush_rows = max(1, flush_rows)
        self.flush_interval_seconds = max(0.1, flush_interval_seconds)
        self.clock_ns = clock_ns
        self._snapshot_rows: List[Dict[str, Any]] = []
        self._trade_rows: List[Dict[str, Any]] = []
        self._last_flush_ns = self.clock_ns()

    def write_snapshots(self, rows: Iterable[Mapping[str, Any]]) -> None:
        self._snapshot_rows.extend(dict(row) for row in rows)

    def write_trades(self, rows: Iterable[Mapping[str, Any]]) -> None:
        self._trade_rows.extend(dict(row) for row in rows)

    def maybe_flush(self) -> List[Path]:
        total_rows = len(self._snapshot_rows) + len(self._trade_rows)
        elapsed_seconds = (self.clock_ns() - self._last_flush_ns) / 1_000_000_000
        if total_rows >= self.flush_rows or (total_rows and elapsed_seconds >= self.flush_interval_seconds):
            return self.flush()
        return []

    def flush(self) -> List[Path]:
        written: List[Path] = []
        if self._snapshot_rows:
            written.extend(_write_partitioned_rows(self._snapshot_rows, self.snapshot_root, SNAPSHOT_COLUMNS))
            self._snapshot_rows = []
        if self._trade_rows:
            written.extend(_write_partitioned_rows(self._trade_rows, self.trade_root, TRADE_COLUMNS))
            self._trade_rows = []
        self._last_flush_ns = self.clock_ns()
        return written


class Hip4CaptureRunner:
    def __init__(
        self,
        config: CaptureConfig,
        *,
        writer: Optional[PartitionedParquetWriter] = None,
        gap_tracker: Optional[GapTracker] = None,
        ws_factory: Optional[Callable[[str], Any]] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.config = config
        self.writer = writer or PartitionedParquetWriter(
            snapshot_root=config.snapshot_root,
            trade_root=config.trade_root,
            flush_rows=config.flush_rows,
            flush_interval_seconds=config.flush_interval_seconds,
        )
        self.gap_tracker = gap_tracker or GapTracker(gap_warn_seconds=config.gap_warn_seconds, logger=logger)
        self.ws_factory = ws_factory or _create_websocket
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.logger = logger
        self.markets_by_coin = {market.coin: market for market in config.markets}
        self.backoff = ExponentialBackoff(config.backoff)

    def run(
        self,
        *,
        seconds: Optional[float] = None,
        max_messages: Optional[int] = None,
        max_reconnects: Optional[int] = None,
    ) -> None:
        deadline = self.monotonic_fn() + seconds if seconds is not None else None
        reconnects = 0
        processed_messages = 0

        while True:
            if deadline is not None and self.monotonic_fn() >= deadline:
                break
            if max_messages is not None and processed_messages >= max_messages:
                break

            try:
                processed_messages += self._stream_once(
                    deadline=deadline,
                    max_messages=None if max_messages is None else max_messages - processed_messages,
                )
                self.backoff.reset()
                if deadline is not None or max_messages is not None:
                    break
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # pragma: no cover - concrete exceptions depend on websocket-client internals.
                self.writer.flush()
                if max_reconnects is not None and reconnects >= max_reconnects:
                    self.logger.warning("HIP-4 capture stopped after reconnect limit: %s", exc)
                    break
                sleep_seconds = self.backoff.next_sleep()
                self.logger.warning(
                    "HIP-4 WebSocket disconnected: %s; reconnecting in %.2fs from last seq %s",
                    exc,
                    sleep_seconds,
                    self.gap_tracker.state_summary(),
                )
                reconnects += 1
                self.sleep_fn(sleep_seconds)

        self.writer.flush()

    def _stream_once(self, *, deadline: Optional[float], max_messages: Optional[int]) -> int:
        ws = self.ws_factory(self.config.websocket_url)
        processed = 0
        try:
            self._subscribe(ws)
            while True:
                if deadline is not None and self.monotonic_fn() >= deadline:
                    return processed
                if max_messages is not None and processed >= max_messages:
                    return processed

                raw = ws.recv()
                if raw == "PING":
                    ws.send("PONG")
                    continue
                self._handle_raw(raw)
                self.writer.maybe_flush()
                processed += 1
        finally:
            try:
                ws.close()
            except Exception:
                self.logger.debug("HIP-4 WebSocket close failed", exc_info=True)

    def _subscribe(self, ws: Any) -> None:
        for market in self.config.markets:
            for channel in self.config.channels:
                ws.send(json.dumps({"method": "subscribe", "subscription": {"type": channel, "coin": market.coin}}))

    def _handle_raw(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.debug("Skipping non-JSON HIP-4 WebSocket payload")
            return
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._handle_message(item)
            return
        if isinstance(payload, dict):
            self._handle_message(payload)

    def _handle_message(self, payload: Mapping[str, Any]) -> None:
        channel = str(payload.get("channel") or "")
        if channel in {"subscriptionResponse", "pong"}:
            return

        data = payload.get("data")
        if channel == "l2Book" and isinstance(data, Mapping):
            rows = normalize_l2_book(
                data,
                self.markets_by_coin,
                venue=self.config.venue,
                max_levels=self.config.max_book_levels,
            )
            if not rows:
                return
            first = rows[0]
            if self._accept_event("l2_snapshots", str(first["market_uid"]), int(first["seq"])):
                self.writer.write_snapshots(rows)
            return

        if channel == "trades":
            trade_payloads = data if isinstance(data, list) else [data]
            rows = []
            for trade in trade_payloads:
                if not isinstance(trade, Mapping):
                    continue
                row = normalize_trade(trade, self.markets_by_coin, venue=self.config.venue)
                if row and self._accept_event("l2_trades", str(row["market_uid"]), int(row["seq"])):
                    rows.append(row)
            self.writer.write_trades(rows)

    def _accept_event(self, stream: str, market_uid: str, seq: int) -> bool:
        if self.gap_tracker.is_stale(stream, market_uid, seq):
            self.logger.debug("Skipping stale HIP-4 %s event for %s at seq %s", stream, market_uid, seq)
            return False
        self.gap_tracker.observe(stream, market_uid, seq)
        return True


def load_capture_config(path: Path = DEFAULT_CONFIG_PATH) -> CaptureConfig:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not isinstance(payload, dict):
        payload = {}

    data_root = Path(str(payload.get("data_root") or "data/raw"))
    markets_payload = payload.get("markets") or []
    markets = tuple(Hip4Market.from_config(item) for item in markets_payload if isinstance(item, Mapping))
    if not markets:
        raise ValueError(f"No HIP-4 markets configured in {path}")

    channels = tuple(str(channel) for channel in (payload.get("channels") or ["l2Book", "trades"]))
    unsupported = set(channels) - {"l2Book", "trades"}
    if unsupported:
        raise ValueError(f"Unsupported HIP-4 capture channels: {sorted(unsupported)}")

    reconnect = payload.get("reconnect") or {}
    if not isinstance(reconnect, Mapping):
        reconnect = {}

    return CaptureConfig(
        venue=str(payload.get("venue") or "hip4"),
        websocket_url=str(payload.get("websocket_url") or DEFAULT_WS_URL),
        markets=markets,
        channels=channels,
        snapshot_root=Path(str(payload.get("snapshot_root") or data_root / "l2_snapshots")),
        trade_root=Path(str(payload.get("trade_root") or data_root / "l2_trades")),
        max_book_levels=int(payload.get("max_book_levels") or 10),
        flush_rows=int(payload.get("flush_rows") or 1000),
        flush_interval_seconds=float(payload.get("flush_interval_seconds") or 5.0),
        gap_warn_seconds=float(payload.get("gap_warn_seconds") or 5.0),
        backoff=BackoffPolicy(
            initial_seconds=float(reconnect.get("initial_backoff_seconds") or 1.0),
            max_seconds=float(reconnect.get("max_backoff_seconds") or 60.0),
            multiplier=float(reconnect.get("multiplier") or 2.0),
        ),
    )


def normalize_l2_book(
    data: Mapping[str, Any],
    markets_by_coin: Mapping[str, Hip4Market],
    *,
    venue: str = "hip4",
    max_levels: int = 10,
) -> List[Dict[str, Any]]:
    coin = str(data.get("coin") or "").strip()
    market = markets_by_coin.get(coin, Hip4Market(coin=coin, market_uid=f"{venue}:{coin}"))
    ts_ns = _event_ts_ns(data)
    seq = _event_seq(data)
    levels = data.get("levels") or []
    rows: List[Dict[str, Any]] = []
    for side_index, side in enumerate(("bid", "ask")):
        side_levels = levels[side_index] if isinstance(levels, list) and len(levels) > side_index else []
        if not isinstance(side_levels, list):
            continue
        for level, item in enumerate(side_levels[:max_levels]):
            if not isinstance(item, Mapping):
                continue
            px = _optional_float(item.get("px"))
            sz = _optional_float(item.get("sz"))
            if px is None or sz is None:
                continue
            rows.append(
                {
                    "ts_ns": ts_ns,
                    "venue": venue,
                    "market_uid": market.market_uid,
                    "seq": seq,
                    "side": side,
                    "level": level,
                    "px": px,
                    "sz": sz,
                }
            )
    return rows


def normalize_trade(
    trade: Mapping[str, Any],
    markets_by_coin: Mapping[str, Hip4Market],
    *,
    venue: str = "hip4",
) -> Optional[Dict[str, Any]]:
    coin = str(trade.get("coin") or "").strip()
    market = markets_by_coin.get(coin, Hip4Market(coin=coin, market_uid=f"{venue}:{coin}"))
    px = _optional_float(trade.get("px"))
    sz = _optional_float(trade.get("sz"))
    if px is None or sz is None:
        return None
    return {
        "ts_ns": _event_ts_ns(trade),
        "venue": venue,
        "market_uid": market.market_uid,
        "seq": _event_seq(trade),
        "side": _trade_side(trade.get("side")),
        "px": px,
        "sz": sz,
        "taker_id_hash": _taker_id_hash(trade),
    }


def _event_ts_ns(payload: Mapping[str, Any]) -> int:
    timestamp_ms = _optional_int(payload.get("time"))
    if timestamp_ms is not None:
        return timestamp_ms * 1_000_000
    return time.time_ns()


def _event_seq(payload: Mapping[str, Any]) -> int:
    for key in ("seq", "sequence", "time", "tid"):
        value = _optional_int(payload.get(key))
        if value is not None:
            return value
    return _event_ts_ns(payload) // 1_000_000


def _trade_side(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"b", "buy", "bid"}:
        return "buy"
    if normalized in {"a", "s", "sell", "ask"}:
        return "sell"
    return normalized or "unknown"


def _taker_id_hash(trade: Mapping[str, Any]) -> Optional[str]:
    for key in ("taker", "taker_id", "takerId", "taker_user", "takerUser"):
        value = trade.get(key)
        if value:
            return hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    return None


def _optional_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _write_partitioned_rows(rows: Sequence[Mapping[str, Any]], root: Path, columns: Sequence[str]) -> List[Path]:
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = {}
    for row in rows:
        venue = str(row["venue"])
        date = _partition_date(int(row["ts_ns"]))
        grouped.setdefault((venue, date), []).append(row)

    written: List[Path] = []
    for (venue, date), group in grouped.items():
        partition_dir = root / f"venue={venue}" / f"date={date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        path = partition_dir / f"part-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:10]}.parquet"
        frame = pd.DataFrame(group, columns=list(columns))
        _apply_schema(frame, columns)
        _copy_dataframe_to_parquet(frame, path)
        written.append(path)
    return written


def _apply_schema(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    frame["ts_ns"] = frame["ts_ns"].astype("int64")
    frame["venue"] = frame["venue"].astype("object")
    frame["market_uid"] = frame["market_uid"].astype("object")
    frame["seq"] = frame["seq"].astype("int64")
    frame["side"] = frame["side"].astype("object")
    frame["px"] = frame["px"].astype("float64")
    frame["sz"] = frame["sz"].astype("float64")
    if "level" in columns:
        frame["level"] = frame["level"].astype("int32")
    if "taker_id_hash" in columns:
        frame["taker_id_hash"] = frame["taker_id_hash"].astype("object")


def _copy_dataframe_to_parquet(frame: pd.DataFrame, path: Path) -> None:
    con = duckdb.connect(":memory:")
    try:
        con.register("rows_df", frame)
        escaped = str(path).replace("'", "''")
        con.execute(f"COPY rows_df TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()


def _partition_date(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).date().isoformat()


def _create_websocket(url: str) -> Any:
    import websocket

    return websocket.create_connection(url, timeout=15)


@app.command("run")
def run_command(
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="HIP-4 capture YAML config."),
    seconds: Optional[float] = typer.Option(None, "--seconds", help="Optional bounded capture duration for smoke tests."),
    log_level: str = typer.Option("INFO", "--log-level", help="Python logging level."),
) -> None:
    """Run read-only HIP-4 L2 and trades capture until interrupted."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    capture_config = load_capture_config(config)
    LOGGER.info(
        "Starting HIP-4 capture for %d market(s), channels=%s, snapshot_root=%s, trade_root=%s",
        len(capture_config.markets),
        ",".join(capture_config.channels),
        capture_config.snapshot_root,
        capture_config.trade_root,
    )
    runner = Hip4CaptureRunner(capture_config)
    try:
        runner.run(seconds=seconds)
    except KeyboardInterrupt:
        LOGGER.info("Stopping HIP-4 capture after interrupt")
        runner.writer.flush()
        raise typer.Exit(0)


if __name__ == "__main__":
    app()
