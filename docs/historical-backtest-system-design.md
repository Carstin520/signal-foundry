# Historical Backtest System Design

Date: 2026-05-17

## 1. Goal

The system should let Signal Foundry run large, cheap, repeatable historical tests before spending more API budget or moving toward any execution layer.

The key research question is:

> When a prediction market reprices, which public information events appeared before, during, or after the repricing window, and can those events be turned into a repeatable, cost-adjusted signal?

This design is not only post-first. It should support two complementary directions:

1. **Price-first backtest**: detect market/盘口波动 first, then search for public posts and news around those windows.
2. **Source-first backtest**: start from X/Reddit/news posts, then measure whether the market repriced afterward.

The price-first path is more economical because it limits expensive X full-archive calls to windows where the market actually moved.

## 2. Data Reality

### Polymarket

Polymarket exposes three useful public read paths:

- Gamma API: market/event discovery and metadata.
- Data API: public user positions, activity, trades, holder data, and analytics.
- CLOB API: orderbook, midpoint, spread, prices, and price history.

Historical CLOB price data is minute-level in practice. The `/prices-history` and `/batch-prices-history` APIs support `fidelity` expressed in minutes, with default 1 minute. This is enough for broad event alignment, but not enough to prove a 1s/10s edge.

For true sub-minute evidence, the system must use live WebSocket/tick capture. The public market WebSocket can stream orderbook snapshots, price changes, last trade prices, and best bid/ask updates without trading credentials.

### X

X recent search covers the last 7 days. Full-archive search goes back to 2006 but requires pay-per-use or Enterprise access. Counts endpoints should be used before fetching full posts so the system can estimate API spend.

### Reddit

Reddit can be used as a secondary social context source, but it should not be treated as the same quality of timestamped alpha as X unless the system can reliably collect exact post times, authors, subreddit context, and historical completeness. Reddit is useful for narrative diffusion, not first-pass source ranking.

## 3. Core Architecture

The backtest system should have six layers.

### 3.1 Market Universe Layer

Purpose: identify markets worth studying before fetching expensive social data.

Inputs:

- Gamma markets/events.
- Current liquidity, volume, tags, end time, active/resolved state.
- CLOB token IDs.

Outputs:

- `market_universe_snapshots`
- `event_families`
- `case_markets`

Selection rules:

- keep active or recently resolved high-liquidity markets;
- prefer multi-maturity event families;
- prefer narrative-driven markets;
- exclude very thin markets unless explicitly studying manipulation;
- mark near-deadline markets separately;
- keep market snapshots by date so historical backtests do not accidentally use future metadata.

### 3.2 Price Event Layer

Purpose: find the timestamps where the market actually repriced.

Inputs:

- CLOB historical minute bars from `/batch-prices-history`.
- Live WebSocket/tick data when available.
- Best bid/ask/spread snapshots when available.

Outputs:

- `price_bars`
- `orderbook_snapshots`
- `price_events`

Price event detection should generate candidate windows using:

- absolute move threshold: e.g. `>= 3pp` in 10m/30m/2h;
- volatility z-score: move relative to rolling market volatility;
- spread/liquidity filter: ignore moves with unusable spread;
- jump filter: detect discontinuous one-bar jumps;
- ramp filter: detect monotonic or stair-step repricing;
- reversal filter: detect FOMO spike then pullback.

Each `price_event` should include:

- `event_id`
- `market_slug`
- `token_id`
- `start_at`
- `peak_at`
- `end_at`
- `direction`
- `move_size`
- `duration_seconds`
- `pre_event_volatility`
- `spread_at_start`
- `liquidity_at_start`
- `price_data_resolution`
- `event_type`: `jump`, `ramp`, `reversal`, `chop`, `slow_trend`

Minute data can identify 5m+ ramps. It cannot validate 1s/10s price-in.

### 3.3 Information Event Layer

Purpose: gather public information around price events without uncontrolled API spend.

Inputs:

- X recent/full-archive search.
- X counts endpoints.
- Current watchlist accounts.
- Candidate account discovery from high-volume topics.
- Optional Reddit posts for context.
- Manual CSV imports.

Outputs:

- `source_posts`
- `source_post_raw_snapshots`
- `source_account_snapshots`
- `social_volume_buckets`

Cost control workflow:

1. Build a small query for each price event:
   - market keywords;
   - entities;
   - source handles;
   - related event terms;
   - language constraints.
2. Call counts first.
3. Fetch posts only if count is within configured bounds.
4. Store raw hashes and dedupe.
5. Never fetch broad full-archive queries without a preflight budget.

Example window:

- price event start: `T`
- source search window: `T - 6h` to `T + 30m`
- for slow political/geopolitical markets, optionally `T - 24h` to `T + 2h`

The default should be narrow. Wider windows should be case-specific.

### 3.4 Matching Layer

Purpose: connect posts to markets and price events.

Inputs:

- Market titles/rules/tags.
- Case seed concepts.
- Source post text.
- Source role and reliability.
- Price event entities and direction.

Outputs:

- `post_market_matches`
- `post_price_event_matches`
- `match_rejections`

Matching should be two-stage:

1. High-precision anchors:
   - exact entity names;
   - market-specific keywords;
   - official resolution terms;
   - known handles.
2. Semantic recall:
   - cloud/local multilingual semantic matching;
   - indirect event relation;
   - exclude concepts to remove unrelated geopolitics/noise.

Every match must store:

- method: `keyword`, `semantic`, `cloud`, `manual`;
- confidence;
- matched concepts;
- rejected concepts;
- direction: `bullish`, `bearish`, `volatility`, `watch_only`;
- whether it is an official confirmation source;
- whether it was before or after the price event started.

## 4. Backtest Modes

### 4.1 Price-First Event Study

This is the most important historical mode.

Workflow:

1. Discover high-liquidity markets.
2. Backfill minute price history.
3. Detect price events.
4. For each price event, search X/Reddit in a bounded pre/post window.
5. Match posts to the event.
6. Score which posts/accounts appeared before the move.

Main output:

- “These accounts/posts appeared before the market repriced.”
- “These accounts only posted after the move had already started.”
- “These topics cause volatility but not directional follow-through.”

This mode is cheaper because social search is targeted to known market moves.

### 4.2 Source-First Event Study

This mode tests whether a source is useful if treated as an ideal entry timestamp.

Workflow:

1. Select accounts and case concepts.
2. Backfill posts.
3. Match posts to markets.
4. Pull price windows around post timestamps.
5. Measure forward price path.

Main output:

- post-level `1m/5m/10m/30m/2h` move;
- whether price had already moved before the post;
- cost-adjusted edge;
- reward/risk;
- reversal risk.

Historical minute data can estimate `1m+`; live burst data is required for `1s/10s/30s`.

### 4.3 Known-Event Replay

This mode uses manually curated real-world event timestamps.

Examples:

- official statement;
- leaked negotiation headline;
- regional reporter post;
- military strike report;
- court ruling;
- election polling release.

The purpose is to separate:

- public event timestamp;
- first source timestamp;
- first market move timestamp;
- best tradable entry timestamp.

This is useful for studying whether the market follows X, news wires, official channels, or its own order flow.

## 5. Execution-Realistic Simulation

Backtests should not use midpoint as if it were executable.

The simulator should support three execution assumptions:

1. **Midpoint research mode**
   - useful for exploratory analysis only;
   - never used for final strategy ranking.
2. **Top-of-book mode**
   - entry uses ask for buying Yes / bid for selling;
   - exit uses bid/ask accordingly;
   - includes spread.
3. **Depth-aware mode**
   - uses orderbook snapshots when available;
   - simulates partial fill and price impact;
   - rejects signals if required size exceeds available depth.

Default paper-trade metrics:

- entry delay;
- entry price;
- exit price;
- round-trip spread cost;
- slippage buffer;
- net max favorable excursion;
- net max adverse excursion;
- reward/risk;
- time to 3pp;
- reversal by 30m;
- hit rate by horizon;
- expectancy per trade;
- drawdown by account and market family.

Exit policies to test:

- fixed 1m exit;
- fixed 5m exit;
- fixed 10m exit;
- take-profit at 3pp/5pp/8pp;
- stop-loss at 2pp/4pp;
- trailing stop after max favorable move;
- time stop if no move after 5m.

## 6. Statistical Validation

The system must guard against false discovery. Source ranking is a multiple-testing problem.

Required validation methods:

- walk-forward split by time;
- market-family holdout;
- account holdout;
- bootstrap confidence intervals;
- placebo timestamps;
- shuffled account labels;
- random event windows matched by volatility regime;
- minimum sample-size threshold;
- Bayesian/shrunk hit rates;
- false discovery control when testing many accounts.

Account ranking should separate:

- speed score;
- direction score;
- volatility score;
- tradability score;
- sample confidence;
- false-FOMO rate.

A source should not be promoted from one good case. Promotion requires repeatability.

## 7. Proposed Schema

### Market Tables

`market_universe_snapshots`

- `snapshot_at`
- `market_slug`
- `event_slug`
- `question`
- `category`
- `tags`
- `active`
- `closed`
- `end_time`
- `liquidity`
- `volume`
- `clob_token_ids`
- `raw_json_hash`

`event_families`

- `event_family`
- `description`
- `entities`
- `preferred_market_types`
- `excluded_market_types`
- `seed_concepts`
- `created_at`

`case_markets`

- `case_id`
- `event_family`
- `market_slug`
- `maturity_bucket`
- `priority`
- `status`

### Price Tables

`price_bars`

- `market_slug`
- `token_id`
- `bar_at`
- `mid`
- `best_bid`
- `best_ask`
- `spread`
- `liquidity`
- `tick_source`
- `resolution`

`price_events`

- `event_id`
- `market_slug`
- `token_id`
- `start_at`
- `peak_at`
- `end_at`
- `direction`
- `move_size`
- `duration_seconds`
- `event_type`
- `spread_at_start`
- `liquidity_at_start`
- `price_data_resolution`
- `detection_config_hash`

### Source Tables

`source_accounts`

- `handle`
- `platform`
- `role`
- `language`
- `status`
- `followers`
- `profile_snapshot_at`
- `notes`

`source_posts`

- `platform`
- `post_id`
- `handle`
- `created_at`
- `text`
- `public_metrics`
- `url`
- `raw_json_hash`

`social_volume_buckets`

- `platform`
- `query_hash`
- `bucket_start`
- `bucket_end`
- `post_count`
- `source_count`

### Matching Tables

`post_market_matches`

- `post_id`
- `market_slug`
- `case_id`
- `method`
- `confidence`
- `direction`
- `matched_concepts`
- `rejected_concepts`
- `match_created_at`

`post_price_event_matches`

- `post_id`
- `price_event_id`
- `lead_seconds`
- `relative_position`: `before`, `during`, `after`
- `match_confidence`
- `direction_agrees`

### Backtest Tables

`backtest_runs`

- `run_id`
- `run_type`
- `config_hash`
- `created_at`
- `train_start`
- `train_end`
- `test_start`
- `test_end`
- `notes`

`backtest_samples`

- `run_id`
- `sample_id`
- `post_id`
- `market_slug`
- `price_event_id`
- `entry_at`
- `entry_price`
- `entry_delay_seconds`
- `horizon`
- `future_price`
- `gross_delta`
- `net_delta`
- `max_favorable_delta`
- `max_adverse_delta`
- `reward_to_risk`
- `paper_trade_positive`
- `risk_tags`

`account_backtest_metrics`

- `run_id`
- `handle`
- `sample_size`
- `lead_rate`
- `micro_hit_rate`
- `tradable_hit_rate`
- `false_fomo_rate`
- `median_entry_delay`
- `median_time_to_price_in`
- `expectancy_after_cost`
- `max_drawdown_proxy`
- `sample_confidence`
- `recommended_status`

## 8. CLI Design

### Market And Price

```bash
python3 -m quant_sol.signals discover-backtest-markets \
  --category geopolitics \
  --min-liquidity 100000 \
  --max-markets 50

python3 -m quant_sol.signals backfill-price-bars \
  --markets-from active_backtest_universe \
  --start 2026-03-01 \
  --end 2026-05-17 \
  --interval 1m \
  --fidelity 1

python3 -m quant_sol.signals mine-price-events \
  --markets-from active_backtest_universe \
  --min-move-pp 3 \
  --windows 10m,30m,2h
```

### Social Backfill

```bash
python3 -m quant_sol.signals plan-source-backfill \
  --from-price-events \
  --pre 6h \
  --post 30m \
  --platform x \
  --use-counts \
  --daily-cap 200

python3 -m quant_sol.signals backfill-source-windows \
  --plan latest \
  --platform x \
  --daily-cap 200
```

### Matching And Backtest

```bash
python3 -m quant_sol.signals match-source-events \
  --method cloud \
  --min-confidence 0.70

python3 -m quant_sol.signals run-price-first-backtest \
  --run-name iran_peace_march_may \
  --horizons 1m,5m,10m,30m,2h \
  --execution top-of-book

python3 -m quant_sol.signals run-source-first-backtest \
  --case live_us_iran_permanent_peace_june30 \
  --horizons 1m,5m,10m,30m,2h \
  --execution top-of-book

python3 -m quant_sol.signals score-backtest-accounts \
  --run latest \
  --min-samples 5

python3 -m quant_sol.signals report-backtest \
  --run latest
```

## 9. Implementation Roadmap

### Phase 1: Price-First Historical Backbone

Build:

- `price_bars`;
- `price_events`;
- `backtest_runs`;
- price event mining command;
- report showing top market moves and candidate windows.

Reason:

This produces useful research without spending X full-archive credits.

### Phase 2: Budgeted Source Backfill

Build:

- source backfill planner;
- counts-first X workflow;
- source post ingestion by event window;
- query budget report.

Reason:

This prevents accidental broad X queries.

### Phase 3: Post/Price Matching

Build:

- post to price-event matching;
- semantic matching over event concepts;
- rejection reasons;
- match confidence audit table.

Reason:

This converts raw posts into explainable test samples.

### Phase 4: Execution-Realistic Paper Backtest

Build:

- top-of-book execution mode;
- exit policy engine;
- cost/slippage assumptions;
- reward/risk and drawdown metrics.

Reason:

This is where “market moved” becomes “maybe tradable”.

### Phase 5: Robust Source Ranking

Build:

- account-level metrics with Bayesian shrinkage;
- train/test split;
- placebo tests;
- source tier updater.

Reason:

This prevents overfitting to one event or one account.

### Phase 6: Live Evidence Merge

Build:

- join historical source ranks with live burst results;
- require live micro samples before promoting sub-minute edge;
- compare historical minute-floor result against live second-level evidence.

Reason:

Historical data can find candidates; live data validates the micro edge.

## 10. Decision Rules

A source should be upgraded only when:

- it has enough samples;
- it posts before price movement more often than chance;
- the move is cost-adjusted positive;
- the signal survives placebo windows;
- it works across at least two market families or several events in one family;
- it is not mostly official confirmation after the move.

A market family should be prioritized when:

- liquidity is high;
- spreads are tight;
- repeated news waves occur;
- multiple maturities exist;
- price events are frequent enough for testing;
- source posts often precede repricing.

A signal should be rejected when:

- price moved before the post;
- spread consumes expected edge;
- adverse excursion is too high;
- source match is weak;
- post is stale backfill;
- market is too close to resolution;
- sample belongs only to minute-floor data but claims sub-minute edge.

## 11. Best Near-Term Build Order

The highest ROI next implementation is:

1. Add `price_events` and a `mine-price-events` command.
2. Add `plan-source-backfill` using X counts-first budgeting.
3. Add `post_price_event_matches`.
4. Add `run-price-first-backtest`.
5. Add an account ranking report that separates:
   - before-move sources;
   - during-move amplifiers;
   - after-move commentators;
   - noisy but volatility-relevant accounts.

This will let the project test many markets with limited X API usage.

## 12. References

- Polymarket API overview: https://docs.polymarket.com/api-reference
- Polymarket `/prices-history`: https://docs.polymarket.com/api-reference/markets/get-prices-history
- Polymarket `/batch-prices-history`: https://docs.polymarket.com/api-reference/markets/get-batch-prices-history
- Polymarket market WebSocket: https://docs.polymarket.com/api-reference/wss/market
- X Search Posts: https://docs.x.com/x-api/posts/search/introduction
- X full-archive counts: https://docs.x.com/x-api/posts/get-count-of-all-posts
- Reddit API overview: https://developers.reddit.com/docs/capabilities/server/reddit-api

