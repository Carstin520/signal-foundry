# Web3 Quant Research Map

Date: 2026-05-14

This project is best treated as a lab: build small collectors, simulators, and paper-trading agents before touching live capital. The most promising beginner-to-intermediate path is prediction-market data and arbitrage tooling, because Polymarket and Kalshi expose usable public market/orderbook APIs and the strategy logic can be validated offline.

## Main Findings

1. Prediction-market arbitrage is the best first track.
   - Polymarket exposes Gamma, Data, and CLOB APIs. Gamma/Data are public; CLOB includes public orderbook/pricing endpoints and authenticated trading endpoints.
   - Kalshi exposes public orderbook endpoints. Kalshi orderbooks return YES and NO bids; asks are implied by `1 - opposite_side_bid`.
   - Recent research suggests simple single-market arbs are rare and short-lived, while combinatorial/cross-market arbs exist but are usually liquidity constrained.

2. The current edge is not "find one magic arb"; it is data normalization, market matching, latency, and sizing.
   - Cross-venue prediction-market work is hard mostly because events have different wording, settlement rules, fees, jurisdictions, and tick/contract conventions.
   - A useful project can start as a scanner that identifies candidate equivalent markets and computes executable size after fees and slippage.

3. DeFi/MEV remains interesting but is much more infrastructure-heavy.
   - Backrun/DEX arbitrage requires low-latency state, private bundle submission, simulation, and gas/builder bidding logic.
   - Flashbots' Hindsight is a good reference for offline MEV-share backrun simulation, but it needs archive-node style infrastructure.
   - Uniswap v4 hooks create a new design surface: dynamic fees, custom AMMs, TWAMM-like execution, and new hook-specific MEV/routing behavior.

4. Solana has a more accessible path for execution experiments.
   - Jupiter gives practical APIs for swaps, trigger orders, routing, and managed execution. This is useful for building quote comparison, slippage studies, and routing-quality dashboards.
   - Jupiter Ultra is managed execution; Metis is better if custom transaction composition or CPI matters.

5. Perps and funding/basis strategies are a good second track.
   - Hyperliquid exposes low-latency websocket data and funding/orderbook endpoints, with explicit rate limits.
   - Reasonable experiments: funding-rate capture, perp/spot basis, cross-exchange basis divergence, liquidation/open-interest signals.

## Strategy Backlog

### A. Prediction Market Arbitrage Scanner

Goal: detect executable "buy complete set below $1" or "sell complete set above $1" opportunities.

Inputs:
- Polymarket Gamma markets/events
- Polymarket CLOB books, spreads, midpoints, trades
- Kalshi market metadata and orderbooks

Core calculations:
- Normalize prices to probability units.
- For binary market:
  - YES ask = `1 - best_no_bid`
  - NO ask = `1 - best_yes_bid`
  - Complete-set buy cost = `yes_ask + no_ask`
  - Complete-set sell revenue = `yes_bid + no_bid`
- For multi-outcome market:
  - Buy all mutually exclusive outcomes if sum(best_ask_i) < 1 after fees.
  - Sell all outcomes if sum(best_bid_i) > 1 after fees and borrow/short mechanics are possible.

Hard parts:
- Market equivalence and settlement rule matching.
- Liquidity at depth, not just top of book.
- Fees, withdrawal/bridge cost, KYC/geographic constraints, and stale books.

First implementation:
- Read-only scanner with no trading.
- Store snapshots in SQLite or DuckDB.
- Output ranked opportunities with expected profit, max executable size, age, and source URLs.

### B. Cross-Venue Prediction Market Matcher

Goal: match equivalent Polymarket/Kalshi markets.

Approach:
- Start with sports and crypto markets because external reference data is structured.
- Use deterministic features first: teams, dates, league, event time, condition type, strike/threshold.
- Add embedding similarity only after deterministic filters.
- Require a human-review flag before treating a match as tradable.

Metrics:
- Precision of matched markets.
- Number of tradable matches.
- Price divergence persistence.
- Realizable size after fees.

### C. Prediction Market "News-to-Probability" Agent

Goal: generate fair-value estimates for markets and compare them to orderbook prices.

Useful signals:
- Official event data feeds for sports.
- Crypto price feeds for price-threshold markets.
- News/RSS for politics or macro, but only after building strong source filters.

Methods:
- Calibration benchmark: Brier score, log loss, reliability curves.
- Ensemble forecasts: base rate + market-implied probability + event-specific model.
- Kelly sizing should be capped heavily in paper trading.

Warning:
- LLM-only trading is fragile. Use LLMs for extraction/classification, not as the only probability engine.

### D. Polymarket Microstructure Dashboard

Goal: learn market making and liquidity behavior without taking risk.

Features:
- Spread, depth, orderbook imbalance, trade intensity.
- Fill-side actor tiers if wallet/trade data is available.
- Market lifecycle analysis: creation, news shocks, final-hour behavior, resolution.

Research angle:
- Recent work argues Polymarket's off-chain CLOB prevents address-level quote-lifecycle attribution, so fill-side data is easier to analyze than maker quote behavior.

### E. Solana Quote/Routing Lab

Goal: compare Jupiter routes and on-chain execution quality.

Experiments:
- Query quotes for common pairs and volatile long-tail tokens.
- Compare expected vs executed output.
- Track slippage, route plans, prioritization fees, and failed transactions.
- Build simulated arbitrage checks across Jupiter, Phoenix/OpenBook, Raydium, Orca, Meteora.

Use Jupiter Ultra for managed execution experiments; use Metis or direct DEX SDKs only when custom transaction composition becomes the point of the experiment.

### F. DeFi Liquidation Monitor

Goal: learn liquidation mechanics without racing production bots initially.

Start with Aave:
- Monitor accounts by health factor.
- Simulate repay size, collateral bonus, swap slippage, gas, and close factor.
- Paper trade liquidation candidates.

Do not start with live liquidation execution. It is competitive and latency-sensitive.

### G. MEV Backrun Simulator

Goal: offline learning of MEV math and bundle simulation.

Reference design:
- Ingest swap events or MEV-share hints.
- Simulate user trade impact.
- Compute best reverse route across DEXes.
- Estimate profit after gas and builder bid.

This is more advanced because realistic simulation needs archive state, local EVM simulation, and private submission paths.

## Suggested Repo Structure

```text
quant_sol/
  docs/
    web3-quant-research-2026-05-14.md
  src/
    collectors/
      polymarket.py
      kalshi.py
      jupiter.py
      hyperliquid.py
    normalization/
      prediction_market.py
      orderbook.py
    strategies/
      prediction_complete_set.py
      cross_venue_prediction.py
      funding_basis.py
    storage/
      duckdb_store.py
    notebooks/
      prediction_market_arbs.ipynb
  tests/
```

## 30-Day Plan

Week 1:
- Build Polymarket + Kalshi read-only collectors.
- Store normalized market metadata and top-of-book snapshots.
- Implement binary complete-set math with unit tests.

Week 2:
- Add orderbook-depth execution sizing.
- Build a CLI scanner: `scan-prediction-arbs`.
- Add CSV/Parquet export for analysis.

Week 3:
- Implement market matching for one domain, preferably NBA/MLB or crypto price-threshold markets.
- Add a review file of candidate matched markets.
- Backtest opportunity frequency and duration.

Week 4:
- Build a paper-trading loop with latency timestamps.
- Add risk report: stale-book rate, fill assumptions, fee model, and opportunity half-life.
- Decide whether to add a dashboard or move to Solana/Jupiter route experiments.

## Sources

- Polymarket API docs: https://docs.polymarket.com/api-reference/introduction
- Kalshi orderbook docs: https://docs.kalshi.com/getting_started/orderbook_responses
- Jupiter API rate limits: https://dev.jup.ag/docs/api-rate-limit
- Jupiter Trigger API: https://dev.jup.ag/docs/trigger-api
- Jupiter Ultra docs: https://developers.jup.ag/docs/ultra
- Hyperliquid rate limits: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- Aave liquidations: https://aave.com/help/borrowing/liquidations
- Uniswap v4 hooks: https://developers.uniswap.org/docs/protocols/v4/concepts/hooks
- Flashbots docs: https://docs.flashbots.net/
- Flashbots Hindsight simulator: https://github.com/flashbots/hindsight
- Arbitrage Analysis in Polymarket NBA Markets: https://arxiv.org/abs/2605.00864
- Fill-Side Non-Retail Trading on Polymarket: https://arxiv.org/abs/2605.11640
- PolySwarm prediction-market LLM framework: https://arxiv.org/abs/2604.03888
- Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets: https://arxiv.org/abs/2508.03474
