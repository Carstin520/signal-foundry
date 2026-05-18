<p align="center">
  <h1 align="center">Signal Foundry</h1>
  <p align="center">
    <strong>Read-only prediction-market research OS for measuring narrative-to-price latency.</strong>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Python-3.9%2B-3776ab?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.9+">
    <img src="https://img.shields.io/badge/Storage-DuckDB-fff000?style=for-the-badge" alt="DuckDB">
    <img src="https://img.shields.io/badge/Market-Polymarket-3151ff?style=for-the-badge" alt="Polymarket">
    <img src="https://img.shields.io/badge/Mode-Research_Only-0f766e?style=for-the-badge" alt="Research Only">
    <img src="https://img.shields.io/github/license/Carstin520/signal-foundry?style=for-the-badge" alt="License">
  </p>
</p>

---

Signal Foundry studies whether public narratives can become tradable before prediction markets fully price them in. It collects public Polymarket, wallet, and social data into local DuckDB, then produces reproducible event studies, backtests, account rankings, and research reports.

> Current stage: research-only. Signal Foundry does not trade, sign wallet messages, manage private keys, or submit orders.

## Table of Contents

- [What Is This?](#what-is-this)
- [Core Thesis](#core-thesis)
- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Main Workflows](#main-workflows)
- [Command Map](#command-map)
- [Data and Safety Policy](#data-and-safety-policy)
- [Limitations](#limitations)
- [Testing](#testing)
- [Documentation](#documentation)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [License](#license)

## What Is This?

Signal Foundry is a Python research toolkit for prediction-market information edges. The system is built around one practical question:

> When a public narrative becomes tradable before it becomes certain, can we identify the sources, markets, and time windows where prediction-market price-in behavior is slow enough to measure?

The project focuses on short-horizon price paths, not just final market resolution. The most important evaluation windows are `1m`, `5m`, and `10m`; historical minute-level data can support `1m+` research, while `1s/10s/30s` claims require live burst or WebSocket capture.

## Core Thesis

Prediction markets do not always adjust instantly to non-official but high-signal public information. The target edge is a short-lived FOMO premium:

1. A market has a live narrative with meaningful uncertainty.
2. Relevant posts appear from fast, high-signal sources.
3. The order book still shows inertia.
4. The market later reprices over seconds to minutes as more participants discover the narrative.
5. The position could theoretically be exited before final event risk dominates.

Signal Foundry measures whether this pattern appears repeatedly enough to deserve further research. It does not assume causality from a single post or market move.

## Features

| Feature | Description |
|---|---|
| **Market discovery** | Finds active Polymarket markets by category, keyword, and narrative focus. |
| **Historical price backfill** | Stores local midpoint/tick history for event cases using public market endpoints. |
| **Price-first backtesting** | Mines real market moves first, then searches for public posts around those windows. |
| **Source-first event studies** | Tests whether selected accounts or posts preceded useful price movement. |
| **Semantic and keyword matching** | Links posts to event cases using keyword rules, local embeddings, or cloud semantic scoring. |
| **Account ranking** | Separates early sources, during-move amplifiers, late commentators, and noisy high-frequency accounts. |
| **Wallet research** | Reads public Polymarket wallet positions/activity and computes risk, PnL, and specialization tags. |
| **Live burst capture** | Collects high-frequency ticks after fresh high-confidence signals for true micro price-in evidence. |
| **Telegram alerts** | Sends compact, explainable alerts when configured. |
| **Local reports** | Writes markdown research outputs under `data/reports/`. |

## Architecture

```text
Public market data       Public social data        Public wallet data
Polymarket Gamma/CLOB    X API or CSV/manual       Polymarket Data API
        |                       |                         |
        v                       v                         v
+----------------+      +----------------+        +----------------+
| Market history |      | Source capture |        | Wallet capture |
+----------------+      +----------------+        +----------------+
        |                       |                         |
        +-----------+-----------+-------------------------+
                    |
                    v
          +---------------------+
          | Local DuckDB store  |
          +---------------------+
                    |
        +-----------+-----------+--------------------+
        |                       |                    |
        v                       v                    v
+----------------+      +----------------+   +------------------+
| Event matching |      | Backtest engine|   | Account metrics  |
+----------------+      +----------------+   +------------------+
        |                       |                    |
        +-----------+-----------+--------------------+
                    |
                    v
          +---------------------+
          | Reports and alerts  |
          +---------------------+
```

The research layer is intentionally separate from any future execution layer. If trading support is ever added, it should live behind explicit risk checks, wallet allocation rules, dry-run previews, and a hard kill switch.

## Quick Start

### Requirements

| Requirement | Notes |
|---|---|
| Python | `3.9+` |
| Storage | DuckDB via Python dependency |
| X API | Optional; required only for public X collection |
| Telegram | Optional; required only for alerts |

### Install

```bash
git clone https://github.com/Carstin520/signal-foundry.git
cd signal-foundry

python3 -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
```

Optional local semantic matching:

```bash
pip install -e ".[semantic]"
```

### Verify

```bash
python3 -m quant_sol.signals --help
python3 -m quant_sol.wallets --help
python3 -m pytest
```

## Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Set only the credentials you need:

| Variable | Required For | Notes |
|---|---|---|
| `X_BEARER_TOKEN` | X API checks, profile sync, post collection | Public X data only. |
| `TELEGRAM_BOT_TOKEN` | Telegram alerts | Optional. |
| `TELEGRAM_CHAT_ID` | Telegram alerts | Optional. |

Local configuration files:

| File | Purpose |
|---|---|
| `config/api_limits.yaml` | API budget and rate-limit settings. |
| `config/fomo_model.yaml` | Signal scoring and FOMO model defaults. |
| `config/market_keyword_rules.yaml` | Market-to-narrative keyword rules. |
| `config/social_watchlist.yaml` | Social source watchlists. |
| `config/wallet_watchlist.yaml` | Wallet research targets. |
| `config/web3_account_watchlist.yaml` | Web3-native account seeds. |
| `config/web3_narrative_keywords.yaml` | Narrative discovery terms. |

Ignored local files include `.env`, `data/`, DuckDB files, raw API responses, generated reports, and `config/semantic_matching.yaml`.

## Main Workflows

### 1. Price-First Historical Backtest

Use this when you want to start from actual market movement and ask: "Who posted before this move?"

```bash
python3 -m quant_sol.signals discover-event-case \
  --query "US Iran peace deal" \
  --start 2026-03-01 \
  --end 2026-05-17 \
  --case us_iran_peace

python3 -m quant_sol.signals backfill-market-history \
  --case us_iran_peace \
  --interval 1m \
  --fidelity 1

python3 -m quant_sol.signals mine-price-events \
  --case us_iran_peace \
  --windows 10m,30m,2h \
  --min-move-pp 3

python3 -m quant_sol.signals plan-source-backfill \
  --case us_iran_peace \
  --pre 6h \
  --post 30m \
  --platform x \
  --use-counts \
  --daily-cap 200

python3 -m quant_sol.signals match-price-events \
  --case us_iran_peace \
  --method keyword \
  --min-confidence 0.70

python3 -m quant_sol.signals run-price-first-backtest \
  --case us_iran_peace \
  --horizons 1m,5m,10m,30m,2h \
  --execution top-of-book

python3 -m quant_sol.signals report-price-first-backtest --case us_iran_peace
```

The report separates pre-move sources, during-move amplifiers, late commentators, skipped expensive source windows, cost-adjusted samples, and data-quality warnings.

### 2. Source-First Event Study

Use this when you already have accounts or posts and want to test ideal entry timing.

```bash
python3 -m quant_sol.signals backfill-x-history \
  --case us_iran_peace \
  --mode x-api \
  --daily-cap 200 \
  --max-accounts 5 \
  --max-posts-per-account 40

python3 -m quant_sol.signals match-event-posts --case us_iran_peace --method cloud

python3 -m quant_sol.signals run-event-backtest \
  --case us_iran_peace \
  --mode micro \
  --horizons 1m,5m,10m,30m,2h

python3 -m quant_sol.signals report-event-backtest --case us_iran_peace
```

If your X API plan does not support full-archive search, use recent 7-day cases or CSV import rather than pretending historical post coverage is complete.

### 3. Live Micro Capture

Use live monitoring when you need second-level evidence for fresh posts and active markets.

```bash
python3 -m quant_sol.signals monitor-event-live \
  --cases live_us_iran_permanent_peace_may31,live_us_iran_permanent_peace_june30,live_us_iran_permanent_peace_dec31 \
  --iterations 1 \
  --daily-x-cap 200 \
  --min-confidence 0.70
```

Manual burst collection:

```bash
python3 -m quant_sol.signals collect-market-burst \
  --case live_us_iran_permanent_peace_june30 \
  --fast-seconds 60 \
  --medium-seconds 600 \
  --slow-seconds 6600
```

### 4. Account Ranking

```bash
python3 -m quant_sol.signals sync-accounts \
  --watchlist web3 \
  --daily-cap 200 \
  --max-accounts 5 \
  --max-posts-per-account 20

python3 -m quant_sol.signals rank-accounts --lookback 30d
python3 -m quant_sol.signals report-accounts --lookback 30d
python3 -m quant_sol.signals export-account-seeds --format csv
```

Single-account source evaluation:

```bash
python3 -m quant_sol.signals evaluate-account-source \
  --handle _FORAB \
  --lookback 7d \
  --daily-cap 200 \
  --max-posts 100
```

This keeps the account as an ad-hoc candidate and writes a local report with profile, narrative coverage, market links, price impact samples, tradability, provenance, and participant-lens review.

### 4.5 Source Discovery V2

Run the preflight-first discovery path when you want current source candidates before spending X API calls:

```bash
python3 -m quant_sol.signals discover-signal-sources \
  --focus narrative \
  --max-markets 10 \
  --daily-cap 200 \
  --reddit-limit 10 \
  --include-public-seeds
```

The public seed layer is configured in `config/source_discovery_v2.yaml`. It maps market terms to X accounts, low-confidence Reddit contexts, and manual/public Discord watch targets. Reports mark public seeds as preflight hints until repeated post-to-price evidence validates lead time.

### 5. Wallet Research

```bash
python3 -m quant_sol.wallets fetch --all
python3 -m quant_sol.wallets analyze --all
python3 -m quant_sol.wallets report --all
```

The wallet collector is read-only. It does not use private keys, trading APIs, or wallet signatures.

## Command Map

| Goal | Command |
|---|---|
| Check X API setup | `python3 -m quant_sol.signals check-api --service x --handle WuBlockchain` |
| Discover markets | `python3 -m quant_sol.signals discover-markets --category politics` |
| Discover signal sources | `python3 -m quant_sol.signals discover-signal-sources --focus narrative --max-markets 8 --daily-cap 200 --include-public-seeds` |
| Evaluate one account source | `python3 -m quant_sol.signals evaluate-account-source --handle _FORAB --lookback 7d --daily-cap 200` |
| Diagnose scoring/model setup | `python3 -m quant_sol.signals diagnose-model` |
| Mine historical price events | `python3 -m quant_sol.signals mine-price-events --case <case>` |
| Plan bounded source search | `python3 -m quant_sol.signals plan-source-backfill --case <case>` |
| Match posts to price events | `python3 -m quant_sol.signals match-price-events --case <case>` |
| Run price-first backtest | `python3 -m quant_sol.signals run-price-first-backtest --case <case>` |
| Report price-first backtest | `python3 -m quant_sol.signals report-price-first-backtest --case <case>` |
| Fetch wallet data | `python3 -m quant_sol.wallets fetch --all` |

## Data and Safety Policy

Signal Foundry is designed as a local public-data research system.

It does not:

- submit trades;
- hold private keys;
- sign wallet messages;
- read cookies, DMs, or private data;
- scrape private pages;
- claim proof of insider information.

Local-only data:

- raw API responses;
- DuckDB databases;
- generated reports;
- `.env`;
- semantic matching credentials/config.

These files are intentionally ignored by git.

## Limitations

- Historical Polymarket price history is effectively minute-level. Treat `1s/10s/30s` historical conclusions as invalid unless backed by live tick/WebSocket data.
- X full-archive search may require paid access. The system uses API caps and counts-first planning to avoid uncontrolled usage.
- Midpoint movement is not the same as executable PnL. Backtests include spread/slippage proxies, but live order book depth and fills are not fully modeled yet.
- Account ranking is probabilistic research, not proof of causality.
- Future trading API support, if built, should be separate from this research layer and must require explicit risk gating.

## Testing

```bash
python3 -m pytest
```

Current coverage includes:

- wallet config, client, storage, analysis, and reporting;
- FOMO scoring and model diagnostics;
- semantic matching;
- source-first event backtests;
- price-first historical backtests;
- Telegram payload handling;
- CLI dry-run safety paths.

## Documentation

- [Long-Term Research Goal](goal/long-term-research-goal.md)
- [Historical Backtest System Design](docs/historical-backtest-system-design.md)
- [Prediction Market Feasibility and Wallet Tracking Plan](docs/prediction-market-feasibility-and-wallet-tracking-plan.md)
- [Project Status](progress/2026-05-16-project-status.md)
- [Finance Model Repair Notes](progress/2026-05-17-finance-model-repair.md)

## Troubleshooting

**Q: X commands fail with authentication errors.**  
A: Set `X_BEARER_TOKEN` in `.env`, then run `python3 -m quant_sol.signals check-api --service x --handle WuBlockchain`.

**Q: Historical backtests show no early posts.**  
A: Check whether your X API plan supports the needed time range. If not, use recent cases or CSV imports and mark coverage as incomplete.

**Q: A report mentions `minute_floor`.**  
A: The data is not suitable for sub-minute claims. Use live burst/WebSocket capture for `1s/10s/30s` evidence.

**Q: Price movement looks large but not tradable.**  
A: Spread, sparse ticks, missing liquidity, or slippage assumptions can erase the edge. Ranking should prefer cost-adjusted samples.

**Q: DuckDB or raw API files appear locally. Should they be committed?**  
A: No. Keep raw data, reports, `.env`, and DuckDB files local.

## Roadmap

- Improve market selection with liquidity, spread, maturity, volatility, and narrative-sensitivity scoring.
- Expand high-timeliness source discovery across independent journalists, OSINT, Web3-native curators, and market translators.
- Collect more live burst samples for real micro price-in validation.
- Add deeper paper-trading simulation with order book depth, partial fills, exits, stops, and drawdown.
- Keep any future execution layer separate, explicit, wallet-scoped, and kill-switch protected.

## License

This project is licensed under the [MIT License](LICENSE).
