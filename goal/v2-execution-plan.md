# Signal Foundry v2 — Cross-Venue Microstructure Research Plan

Date: 2026-05-19
Status: read-only research, no execution
Supersedes: nothing (complements `goal/long-term-research-goal.md` by narrowing the active research focus)

## 0. North Star (Revised)

Tighten the research question from "single-venue narrative price-in" to:

> Across three structurally different prediction venues — Polymarket, Kalshi, and Hyperliquid HIP-4 — when the implied probability of the same event family diverges beyond friction costs and rule-mismatch tolerance, does a repeatable, quantifiable, paper-executable short-horizon convergence edge exist? In addition, can account-level signals combined with L2 microstructure evidence anticipate these divergences before they fully form?

This sharpened mission satisfies three criteria simultaneously:

- **Profitability potential** — spread edge is bounded, not open-ended noise chasing.
- **Novelty** — HIP-4 outcome microstructure has no public quantitative literature.
- **Research value** — produces publishable findings on cross-venue price discovery and information cascade.

## 1. Three Research Anchors (Mutually Reinforcing, Not Parallel Tracks)

### Anchor A — Cross-Venue Spread and Rule Normalization (structural edge)

Bounded, quantifiable, paper-grade. Answers: *when is a price spread anomalous rather than structural?*

### Anchor B — Microstructure Anomalies and Social Confluence (event edge)

High alpha, lower sample size. Answers: *did informed participants begin positioning before the divergence appeared in public?*

### Anchor C — Account Market Impact Scoring (signal quality)

Replaces simple hit-rate with a Market Impact triplet: Lead Time, Price Delta, Reversion Rate. Feeds high-quality social signals into Anchor B.

The three anchors share one data foundation (Section 2) and one paired-event registry (Section 3.1).

---

## 2. Data Layer Upgrade (Phase 0, highest priority)

### 2.1 Storage stack

Do **not** introduce TimescaleDB or QuestDB at this stage (operational cost not justified at current write rates). Use:

| Layer | Tool | Purpose |
|---|---|---|
| Raw L2 / trades | Parquet, partitioned by `venue/date=YYYY-MM-DD` | High-write, columnar, compressed, no migration |
| Derived features, registries, event tables | DuckDB | Analytical queries, joins, reports |
| Time-series analysis | DuckDB queries over Parquet directly | Native support, zero ops |

Revisit QuestDB only if HIP-4 sustained ingest exceeds ~50k events/sec or DuckDB+Parquet measurably bottlenecks aggregation. Avoid premature infrastructure.

### 2.2 Unified schema

```sql
-- raw L2 (Parquet, partitioned)
l2_snapshots(
  ts_ns BIGINT,           -- nanosecond timestamp
  venue TEXT,             -- 'polymarket' | 'kalshi' | 'hip4'
  market_uid TEXT,        -- globally unique id: 'venue:native_id'
  seq BIGINT,             -- venue-provided sequence number (gap detection)
  side TEXT,              -- 'bid' | 'ask'
  level INT,              -- 0..9
  px DOUBLE,              -- probability units 0..1
  sz DOUBLE
)

l2_trades(
  ts_ns, venue, market_uid, seq,
  side, px, sz,
  taker_id_hash TEXT      -- nullable, if exposed by venue
)

-- derived microstructure (DuckDB, 1-second aggregates)
microstructure_1s(
  ts_s BIGINT, venue, market_uid,
  mid DOUBLE,
  spread_bps DOUBLE,
  top1_imbalance DOUBLE,        -- (bid_sz1 - ask_sz1) / (bid_sz1 + ask_sz1)
  top3_imbalance DOUBLE,
  depth_top3_bid DOUBLE,
  depth_top3_ask DOUBLE,
  micro_price DOUBLE,           -- size-weighted mid
  micro_momentum_5s DOUBLE,     -- first difference of micro_price over 5s
  cancel_rate_5s DOUBLE,        -- cancels / total book updates within 5s
  taker_volume_5s DOUBLE,
  taker_imbalance_5s DOUBLE     -- (buy_vol - sell_vol) / total_vol
)
```

### 2.3 Capture requirements

| Venue | Source | Frequency | Notes |
|---|---|---|---|
| Polymarket | CLOB WebSocket + REST midpoint | Full L2 subscription | Existing baseline; add sequence parsing and cancel-event detection |
| Kalshi | REST orderbook polling | 1 second | No public WS; REST is acceptable for now |
| HIP-4 | `l2Book` WS + `trades` WS + `allMids` | Full L2 subscription | **Largest current data debt** |

**Phase 0 Definition of Done**:

- 5 paired markets capture continuously for 72 hours with no gap larger than 5 seconds (gaps log a warning).
- `microstructure_1s` is automatically materialized from raw L2.
- A data-integrity check script is added to CI.

---

## 3. Anchor A — Cross-Venue Spread and Rule Normalization

### 3.1 Paired event registry

```sql
paired_event_registry(
  pair_id TEXT PRIMARY KEY,
  event_family TEXT,              -- 'us_iran_nuclear', 'btc_etf_decision', ...
  polymarket_market_uid TEXT,
  kalshi_market_uid TEXT,
  hip4_market_uid TEXT,
  deadline_utc TIMESTAMP,
  resolution_source_type TEXT,    -- 'official_statement' | 'oracle' | 'price_feed' | 'committee'
  resolution_source_url TEXT,
  dispute_mechanism TEXT,         -- 'uma' | 'cftc_regulated' | 'builder_code'
  early_resolution_possible BOOL,
  known_rule_mismatch_tags TEXT[],
  manual_reviewer TEXT,
  reviewed_at TIMESTAMP
)
```

At least 20 pairs must be hand-annotated before any cross-venue comparison report is produced. Title-only matching is forbidden — every comparison must traverse this registry.

### 3.2 Friction cost model and no-arbitrage band

```sql
friction_cost_model(
  venue TEXT,
  market_uid TEXT,
  taker_fee_bps DOUBLE,
  maker_rebate_bps DOUBLE,
  funding_cost_apr DOUBLE,        -- Kalshi: USD T-bill yield; Polymarket: USDC opportunity cost
  expected_settlement_delay_days DOUBLE,
  dispute_risk_premium_bps DOUBLE -- Empirical, calibrated from historical UMA disputes / Kalshi resolution delays
)

no_arb_band(
  pair_id TEXT,
  asof TIMESTAMP,
  band_bps_lower DOUBLE,
  band_bps_upper DOUBLE,
  components JSON                  -- breakdown: fee, funding, dispute, settlement_delay
)
```

**Core rule**: a cross-venue spread is treated as an *event-driven anomaly* only when `|spread| > band_upper`. Anything inside the band is *structural* and is logged as context, never as a strategy trigger.

### 3.3 Spread time series and anomaly events

```sql
pair_spread_1s(
  ts_s, pair_id,
  implied_prob_a, implied_prob_b, spread_bps,
  rolling_mean_bps, rolling_std_bps, z_score
)

spread_anomaly_events(
  event_id, pair_id, opened_at, closed_at,
  peak_z_score, peak_spread_bps,
  leader_venue TEXT,              -- which venue moved first
  follower_venue TEXT,
  convergence_seconds INT,        -- time until spread returned within band
  outcome_label TEXT              -- 'converged' | 'diverged' | 'expired' | 'rule_event'
)
```

**Definition of Done**: at least 200 spread anomaly events accumulated; the lead-lag matrix can be rendered as (venue pair × event category) → (leader frequency, mean lead seconds, mean convergence time, convergence success rate).

---

## 4. Anchor B — Microstructure Anomalies and Social Confluence

### 4.1 Microstructure anomaly detector (from `microstructure_1s`)

The same ruleset runs across all venues:

| Anomaly type | Trigger condition (initial values, calibrate on data) |
|---|---|
| Liquidity drain | `depth_top3` drops more than 50% within 5s and `spread_bps` exceeds 2× rolling median |
| Aggressive directional taker | `|taker_imbalance_5s| > 0.7` sustained for at least 3 seconds |
| Depth imbalance buildup | `|top3_imbalance| > 0.6` sustained for at least 10 seconds |
| Fake wall candidate | Large resting order placed and cancelled within 5 seconds |

```sql
microstructure_events(
  event_id, ts_s, venue, market_uid,
  event_type TEXT, direction TEXT, score DOUBLE,
  context JSON                   -- snapshot of microstructure_1s at trigger
)
```

### 4.2 Social and microstructure confluence

```sql
confluence_events(
  event_id,
  social_post_id, social_author, social_ts,
  microstructure_event_id, microstructure_ts,
  pair_id, direction,
  social_to_micro_lag_seconds INT,
  confidence_tag TEXT            -- 'social_leads_micro' | 'micro_leads_social' | 'simultaneous'
)
```

Working hypothesis: a weak social signal alone is unreliable, and a microstructure anomaly alone is unreliable; jointly, when direction agrees and the time window is within N minutes, combined confidence rises materially.

### 4.3 Feasibility of historical event → account ranking backtest

A clear answer to a recurring question:

| Dimension | Feasibility | Notes |
|---|---|---|
| Use historical Polymarket midpoints to backtest account lead time | Yes, available today | Polymarket publishes historical midpoints |
| Use historical L2 microstructure precedence for backtesting | Partial for Polymarket, infeasible for HIP-4 and Kalshi | No historical L2 archive; especially absent for HIP-4 |
| Walk back from a "market reaction point" and rank accounts that posted just before | Conceptually feasible, but selection-biased | Any reaction point will always have *some* prior tweet; comparing against a control set of non-reactive periods is mandatory |

Correct method: anchor on `spread_anomaly_events` (more objective and denser than narrative reaction points), then look backward 30 minutes into the social stream, and always compare against a control window of similar duration. This is the basis for Anchor C scoring.

---

## 5. Anchor C — Account Market Impact Score (replaces hit-rate)

```sql
account_market_impact(
  account_id, asof_date,
  -- Lead Time
  median_lead_seconds_vs_mainstream DOUBLE,    -- vs Reuters/AP/official baseline pool
  sample_n_lead INT,
  -- Price Delta
  median_price_delta_bps_5m DOUBLE,
  median_price_delta_bps_10m DOUBLE,
  sample_n_delta INT,
  -- Reversion
  reversion_rate_60m DOUBLE,                   -- fraction of moves that reverse >=50% within 60m
  -- Anomaly precedence
  anomaly_precedence_rate DOUBLE,              -- frequency of account appearing within 30m before spread/micro anomaly
  base_rate DOUBLE,                            -- account's baseline posting density mapped to expected appearance rate
  impact_score DOUBLE                          -- composite, formula documented in code
)
```

Hard requirements:

- `anomaly_precedence_rate` must always be reported net of `base_rate` (otherwise high-frequency posters are systematically over-scored).
- Any account with `sample_n_*` below threshold (default 20) is labelled `insufficient_samples` and not ranked.
- The mainstream baseline pool is maintained in `config/settlement_source_accounts.yaml` and includes Reuters, AP, the White House, Treasury, Federal Register, CFTC, and equivalent newswire and official accounts.

**Definition of Done**: 100 candidate accounts scored on the full triplet; at least 30 accounts pass the sample threshold and receive a final `impact_score`.

---

## 6. Paper Trading Simulator (Phase 4 output, gates any future execution work)

### 6.1 Strategy variants

| Strategy | Entry | Exit | Position |
|---|---|---|---|
| S1: Spread reversion | `|z_score| > 2` and `|spread| > band_upper` | Spread returns within band, 24h timeout, or settlement window | Two-sided delta-neutral basket |
| S2: Lead-lag follow | Leader venue moves more than X bps within N seconds | 5m / 10m / 30m tiered exits | One-sided position on lagging venue |
| S3: Confluence entry | `confluence_events` triggered | 5m / 10m fixed + trailing | One-sided directional |

### 6.2 Slippage and fee model

Slippage must be computed by walking each side's recorded L2 book from Phase 0 — no constant assumptions. Fees, funding, and dispute risk all come from `friction_cost_model`.

### 6.3 Definition of Done

- At least 50 paper trades per strategy.
- Cost-adjusted expectancy reported, sliced by event family and venue pair.
- Failure cases classified explicitly: `rule_event`, `liquidity_gap`, `direction_flip`, `settlement_delay`, `data_quality`.
- Each strategy report must include a "do not execute under these conditions" checklist.

Only if S1 or S3 produces a stable, statistically significant cost-adjusted positive expectancy may the project enter the execution-research phase (Phase 6 in `long-term-research-goal.md`).

---

## 7. Phased Roadmap (6 weeks)

| Week | Phase | Deliverable | Definition of Done |
|---|---|---|---|
| W1 | 0a | HIP-4 L2 + trades WebSocket persistence, Parquet partitioning | 3 HIP-4 outcomes captured 72h with no gap |
| W2 | 0b + 1a | Kalshi REST snapshots; `paired_event_registry` schema and 5 hand-annotated pairs | Same event family time-aligned and joinable across venues |
| W2 | 0c | `microstructure_1s` derivation pipeline | Spread, imbalance, momentum, cancel_rate populated for all venues |
| W3 | 1b | `friction_cost_model` and `no_arb_band` | Top 20 pairs fully annotated with rule normalization and band |
| W3 | 2a | `pair_spread_1s` and `spread_anomaly_events` detector | First batch of 50+ events landed |
| W4 | 2b | Lead-lag matrix report | 200+ events, at least 5 event categories covered |
| W4 | 3a | `microstructure_events` detector | Approximately 10 events per venue per active day |
| W5 | 3b | `confluence_events` joint detector | 50+ confluence cases |
| W5 | 3c | `account_market_impact` v1 scoring | 100 candidates ranked, 30+ above sample threshold |
| W6 | 4 | Paper trading simulator across three strategies and backtest report | 50+ trades per strategy, failures classified |

Each week ends with a `progress/YYYY-MM-DD-vX.md` summary documenting deliverables shipped, DoDs missed, blockers, and the next week's corrections.

---

## 8. Global Success Criteria (judges whether v2 itself succeeded)

| Dimension | Metric | Threshold |
|---|---|---|
| Data | 5 paired markets, 72h continuous capture | Pass / Fail |
| Data | `microstructure_1s` coverage of trading hours | ≥ 99% |
| Structural | Spread anomaly events accumulated | ≥ 200 |
| Structural | Event-driven share of total spread observations | Interpretable and < 30% |
| Microstructure | Confluence events accumulated | ≥ 50 |
| Signal | Accounts above sample threshold | ≥ 30 |
| Strategy | At least one strategy with cost-adjusted positive expectancy | One-sided t-test p < 0.05 |
| Safety | End-to-end read-only; no secrets, private keys, or DMs ingested | Pass / Fail |
| Testing | Tracked test pass rate | 100% |

---

## 9. Explicit Out-of-Scope (prevents drift)

- Single-venue Polymarket FOMO point-chasing as a standalone strategy.
- Account ranking by engagement or follower counts.
- Cross-venue matching by title similarity only.
- Treating "price stuck at 80%" as a free arbitrage opportunity without first explaining dispute risk premium.
- Adding TimescaleDB, QuestDB, Kafka, or similar infrastructure unless DuckDB + Parquet measurably fails.
- Any ingestion of trading credentials, private keys, DMs, cookies, or paywalled data.
- Anointing accounts based on a single excellent call; `sample_n` thresholds are non-negotiable.

---

## 10. Compatibility with Existing Project

Preserved and reused:

- `social_watchlist.yaml`, `web3_account_watchlist.yaml` become the candidate pool for Anchor C.
- `market_keyword_rules.yaml` seeds `paired_event_registry`.
- Existing Polymarket midpoint history becomes baseline data prior to Phase 2 anomaly detection.
- The current 87 tracked tests remain a CI gate.

Refactored or deprecated:

- Legacy hit-rate-based account scoring is replaced by `account_market_impact`.
- Legacy cross-venue context based on title alignment is replaced by `paired_event_registry` + `no_arb_band`.
- Legacy single-venue narrative burst trigger is replaced by spread anomaly or confluence-event triggers.

---

## 11. Operating Rules for Agents Executing This Plan

1. Always cite `goal/v2-execution-plan.md` in commit messages and progress reports.
2. Never weaken a Definition of Done to ship faster; document the miss and propose a fix instead.
3. Never silently bypass the `paired_event_registry`. Any cross-venue claim must reference a `pair_id`.
4. Treat every new metric as guilty until base-rate-controlled.
5. Do not introduce new infrastructure components unless explicitly approved in a progress report.
6. Read-only invariants are inviolable: no private keys, no trading APIs, no DMs, no paywalled data.
7. When in doubt about a venue rule (UMA dispute window, Kalshi resolution language, HIP-4 builder code), document the uncertainty in `known_rule_mismatch_tags` rather than guessing.

---

## 12. First Concrete Step

Begin Phase 0a immediately: implement HIP-4 L2 and trades WebSocket persistence into Parquet, partitioned by `venue/date`. This unblocks every downstream anchor. Do not wait on the paired registry; it can be built in parallel.
