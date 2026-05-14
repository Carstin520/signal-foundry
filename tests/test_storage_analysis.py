import json

from quant_sol.wallets.analysis import analyze_wallets
from quant_sol.wallets.config import WATCHLIST
from quant_sol.wallets.reporting import write_markdown_report
from quant_sol.wallets.storage import (
    apply_market_metadata,
    connect,
    latest_metrics,
    missing_event_slugs,
    save_payload,
    seed_wallets,
    upsert_market_metadata,
)


FETCHED_AT = "2026-05-14T00:00:00+00:00"


def test_save_payload_dedupes_raw_snapshots(tmp_path) -> None:
    con = connect(tmp_path / "test.duckdb")
    seed_wallets(con)
    wallet = WATCHLIST["aviato"]
    payload = [{"conditionId": "m1", "title": "Market 1", "pnl": 1.0}]

    first = save_payload(con, wallet, "closed-positions", payload, fetched_at=FETCHED_AT, raw_root=tmp_path / "raw")
    second = save_payload(con, wallet, "closed-positions", payload, fetched_at=FETCHED_AT, raw_root=tmp_path / "raw")
    count = con.execute("select count(*) from raw_api_snapshots").fetchone()[0]

    assert first == second
    assert count == 1


def test_analyze_wallet_metrics_and_risk_tags(tmp_path) -> None:
    con = connect(tmp_path / "test.duckdb")
    seed_wallets(con)
    wallet = WATCHLIST["aviato"]

    save_payload(
        con,
        wallet,
        "closed-positions",
        [
            {
                "conditionId": "m1",
                "eventSlug": "nba-final",
                "title": "NBA Final",
                "outcome": "Yes",
                "pnl": 80,
                "volume": 1000,
            },
            {
                "conditionId": "m2",
                "eventSlug": "election",
                "title": "Election",
                "outcome": "No",
                "pnl": -20,
                "volume": 500,
            },
        ],
        fetched_at=FETCHED_AT,
        raw_root=tmp_path / "raw",
    )
    save_payload(
        con,
        wallet,
        "positions",
        [
            {
                "conditionId": "m3",
                "eventSlug": "open-market",
                "title": "Open Market",
                "outcome": "Yes",
                "pnl": 10,
                "value": 120,
            }
        ],
        fetched_at=FETCHED_AT,
        raw_root=tmp_path / "raw",
    )
    save_payload(
        con,
        wallet,
        "activity",
        [
            {
                "conditionId": "m1",
                "eventSlug": "nba-final",
                "title": "NBA Final",
                "timestamp": "2026-05-13T12:00:00+00:00",
                "endDate": "2026-05-13T18:00:00+00:00",
                "side": "BUY",
                "price": 0.75,
                "size": 100,
            }
        ],
        fetched_at=FETCHED_AT,
        raw_root=tmp_path / "raw",
    )
    assert missing_event_slugs(con, "aviato") == ["election", "nba-final", "open-market"]
    upsert_market_metadata(con, "nba-final", {"tags": [{"label": "Sports"}]}, fetched_at=FETCHED_AT)
    upsert_market_metadata(con, "election", {"tags": [{"label": "Politics"}]}, fetched_at=FETCHED_AT)
    upsert_market_metadata(con, "open-market", {"tags": [{"label": "Sports"}]}, fetched_at=FETCHED_AT)
    apply_market_metadata(con)

    metrics = analyze_wallets(con, ["aviato"])
    metric = metrics[0]

    assert metric.realized_pnl == 60
    assert metric.unrealized_pnl == 10
    assert metric.win_rate == 0.5
    assert metric.closed_markets == 2
    assert metric.open_markets == 1
    assert metric.top_category == "Sports"
    assert round(metric.max_market_pnl_share or 0, 3) == 1.333
    assert "concentrated" in metric.risk_tags
    assert "category_specialist" in metric.risk_tags
    assert "late_entry" in metric.risk_tags


def test_report_includes_four_resolved_and_one_unresolved_wallet(tmp_path) -> None:
    con = connect(tmp_path / "test.duckdb")
    seed_wallets(con)

    analyze_wallets(con, list(WATCHLIST.keys()))
    rows = latest_metrics(con)
    report_path = write_markdown_report(rows, report_root=tmp_path / "reports")
    report = report_path.read_text(encoding="utf-8")

    assert len(rows) == 5
    assert report.count("| aviato |") == 1
    assert report.count("| Annica |") == 1
    assert report.count("| reachingthesky |") == 1
    assert report.count("| GCottrell93 |") == 1
    assert "majorexploiter" in report
    assert "unresolved_wallet" in report
