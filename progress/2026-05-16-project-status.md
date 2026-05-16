# Signal Foundry Project Progress

Date: 2026-05-16

## 1. Current Project Positioning

Signal Foundry has evolved from a simple Web3 quant experiment into a prediction-market research and future execution OS. The current implementation is intentionally read-only, but the long-term product can include a controlled execution layer that connects to prediction-market trading APIs and allocates capital across dedicated wallets.

The current focus is no longer generic arbitrage or final-resolution prediction. The project is now centered on one specific research problem:

> Can public narrative acceleration, especially from fast X accounts, identify a short tradable window before Polymarket fully prices in the story?

The working edge is a FOMO premium:

- a narrative starts forming before official confirmation;
- fast accounts or source clusters discuss it;
- the market still shows inertia;
- price moves over seconds to minutes as more traders discover the narrative;
- the opportunity is evaluated on price path, not final market resolution.

The current system is intentionally read-only:

- no private keys;
- no trading API;
- no auto-betting;
- no private data scraping;
- no claim that suspicious wallet timing proves insider information.

Future scope, after enough paper-trade evidence:

- connect to prediction-market trading APIs;
- allocate trades across dedicated strategy wallets;
- enforce per-wallet, per-market, per-event-family, and per-day risk limits;
- record an audit trail from signal to order to outcome;
- keep signal generation and order execution separated.

## 2. Repository Structure

Main directories:

- `quant_sol/wallets/`: read-only Polymarket wallet collector and wallet analytics.
- `quant_sol/signals/`: prediction-market signal system, X account ranking, semantic matching, event backtests, live monitoring, Telegram alerts.
- `config/`: watchlists, model rules, API usage caps, market keyword rules.
- `docs/`: early research notes and feasibility documents.
- `goal/`: long-term product and research direction.
- `progress/`: current status and development progress notes.
- `tests/`: unit and integration-style tests.
- `data/`: local-only raw payloads, DuckDB, and generated reports. This should stay ignored by git.

Important current files:

- `goal/long-term-research-goal.md`: long-term research goal and roadmap.
- `config/api_limits.yaml`: local X API call caps, currently set to `200` daily calls.
- `config/social_watchlist.yaml`: X accounts used for geopolitical/social signal monitoring.
- `config/web3_account_watchlist.yaml`: Web3/information-speed account watchlist.
- `config/semantic_matching.yaml`: local ignored semantic/cloud matching config. This can contain model/API settings and must not be committed.
- `quant_sol/signals/history.py`: event case discovery, historical/live backtesting, micro price-in logic, paper-trade proxy.
- `quant_sol/signals/semantic.py`: local/cloud semantic matching.
- `quant_sol/signals/cli.py`: main CLI surface.
- `quant_sol/signals/storage.py`: DuckDB schema and persistence.

Future execution files do not exist yet. When added, they should be isolated from the research modules, for example under a separate package such as `quant_sol/execution/`.

## 3. Current CLI Surface

Main signal commands:

```bash
python3 -m quant_sol.signals discover-markets
python3 -m quant_sol.signals sync-social
python3 -m quant_sol.signals check-api
python3 -m quant_sol.signals discover-accounts
python3 -m quant_sol.signals sync-accounts
python3 -m quant_sol.signals sync-follow-graph
python3 -m quant_sol.signals rank-accounts
python3 -m quant_sol.signals report-accounts
python3 -m quant_sol.signals diagnose-model
python3 -m quant_sol.signals discover-event-case
python3 -m quant_sol.signals backfill-market-history
python3 -m quant_sol.signals backfill-x-history
python3 -m quant_sol.signals run-event-backtest
python3 -m quant_sol.signals match-event-posts
python3 -m quant_sol.signals report-event-backtest
python3 -m quant_sol.signals export-account-seeds
python3 -m quant_sol.signals sync-wallets
python3 -m quant_sol.signals stream-market
python3 -m quant_sol.signals sync-market-ticks
python3 -m quant_sol.signals collect-market-ticks
python3 -m quant_sol.signals collect-market-burst
python3 -m quant_sol.signals monitor-event-live
python3 -m quant_sol.signals score
python3 -m quant_sol.signals alert
python3 -m quant_sol.signals evaluate
python3 -m quant_sol.signals report
```

Wallet commands:

```bash
python3 -m quant_sol.wallets fetch
python3 -m quant_sol.wallets analyze
python3 -m quant_sol.wallets report
```

## 4. Data And Local DB Status

Current local DuckDB aggregate counts:

| Table | Count |
| --- | ---: |
| `markets` | 71 |
| `market_ticks` | 26,245 |
| `social_posts` | 316 |
| `x_accounts` | 6 |
| `x_posts` | 80 |
| `event_cases` | 9 |
| `event_case_posts` | 1,356 |
| `post_market_semantic_matches` | 170 |
| `event_post_impacts` | 6,738 |
| `event_account_metrics` | 51 |
| `live_burst_runs` | 1 |
| `wallet_activity` | 0 |

Current tick source mix:

| Tick Source | Count |
| --- | ---: |
| `historical` | 25,457 |
| `live` | 544 |
| `live_burst` | 230 |
| `live_baseline` | 3 |
| `null / legacy` | 11 |

Current local X API usage:

- `api_x_today`: 68
- daily cap: 200
- remaining local cap: 132

Notes:

- `data/` contains generated reports and local DB state. It should not be committed.
- Current raw data and reports are useful for research but should be treated as local artifacts.
- `wallet_activity` is currently empty in the signals DB, so wallet-flow integration into signal scoring is not yet active in the live event workflow.

## 5. Current Event Cases

The local DB currently has these event cases:

| Case ID | Market Slug | Status |
| --- | --- | --- |
| `live_china_taiwan_clash` | `china-x-taiwan-military-clash-before-2027` | `live_watch` |
| `live_china_taiwan_invasion` | `will-china-invade-taiwan-before-2027` | `live_watch` |
| `live_trump_israel_visit` | `will-donald-trump-visit-israel-in-2026` | `live_watch` |
| `live_trump_out_president` | `trump-out-as-president-before-gta-vi-846` | `live_watch` |
| `live_us_iran_nuclear_deal` | `us-iran-nuclear-deal-before-2027` | `live_watch` |
| `live_us_iran_permanent_peace_dec31` | `us-x-iran-permanent-peace-deal-by-december-31-2026-961-587` | `live_watch` |
| `live_us_iran_permanent_peace_june30` | `us-x-iran-permanent-peace-deal-by-june-30-2026-837-641-896-877` | `live_watch` |
| `live_us_iran_permanent_peace_may31` | `us-x-iran-permanent-peace-deal-by-may-31-2026-333-871-241-192` | `live_watch` |
| `trump_china_visit` | `will-trump-visit-china-by-may-15-835-774-595` | `active` |

The main research pool right now is the US-Iran permanent peace deal family, especially the May31 / June30 / Dec31 maturities.

## 6. Completed Capabilities

### 6.1 Wallet Collector V1

Implemented:

- read-only Polymarket wallet collector;
- local raw snapshot storage;
- DuckDB wallet and position tables;
- wallet metrics and report generation;
- unresolved wallet handling;
- tests for wallet config, URL safety, raw hash behavior, wallet metrics, and reporting.

Current limitation:

- signals DB currently does not have active wallet activity populated for event scoring.
- wallet flow is still more of a separate research module than an integrated live signal feature.

### 6.2 X API Setup And API Safety

Implemented:

- `.env` loading without overriding shell env;
- X API token checks;
- local API call budget tracking;
- dry-run estimation;
- cap blocking before calls when over budget;
- X daily cap increased from `80` to `200`.

Current limitation:

- X API is still rate/credit constrained.
- full historical backtests are limited by whether the account has full-archive search access.
- current production-safe path is recent search/timeline plus local CSV fallback.

### 6.3 Web3 / Fast Account Ranking

Implemented:

- Web3 account watchlist config;
- bilingual keyword matching;
- account ranking model;
- speed/frequency/cascade/market-impact/source-chain scoring;
- support for non-Web3 markets using Web3/fast accounts as information-speed sources.

Important clarification:

- Web3 accounts are used because they may be fast at detecting market-moving narratives.
- The project is not limited to Web3 markets.

Current limitation:

- account ranking still needs more valid live micro samples.
- source-chain discovery is not yet fully automated at scale because follow graph collection is API-expensive.

### 6.4 Historical Event Backtest

Implemented:

- event case discovery;
- market history backfill;
- X history backfill;
- post-level event-study;
- account-level metrics;
- report generation.

Supported modes:

- `event`
- `ramp`
- `volatility`
- `micro`

Current limitation:

- Polymarket historical price history is effectively minute-level for this purpose.
- It cannot validate `1s`, `10s`, or `30s` price-in behavior.
- Reports now correctly mark `minute_floor` or `insufficient_resolution`.

### 6.5 Micro Price-In Model

Implemented:

- default micro horizons: `1s`, `10s`, `30s`, `1m`, `5m`, `10m`, `30m`, `2h`;
- primary evaluation emphasis on `1m`, `5m`, and `10m`;
- entry delay measurement;
- time-to-3pp measurement;
- max favorable/adverse move;
- 10m max move;
- 30m reversal;
- late-after-price-move detection;
- stale historical backfill prevention for live burst triggers.

Current limitation:

- true micro validation requires live burst or WebSocket data.
- historical minute-floor data is useful for context but not sufficient for second-level edge validation.

### 6.6 Semantic Matching

Implemented:

- local semantic matching hook;
- cloud semantic matching hook;
- multilingual matching support;
- keyword fallback;
- matched/rejected concepts;
- warning behavior when local dependency or cloud key is unavailable;
- cloud rate-limit handling.

Current behavior:

- `config/semantic_matching.yaml` is ignored because it may contain local/cloud model settings or API credentials.
- semantic matching is used to widen recall beyond simple keyword matching.
- direction is still rule-based for auditability.

Current limitation:

- semantic matching can add relevant old posts from backfill, so live triggers must check post freshness.
- this freshness guard has been implemented.

### 6.7 Live Monitoring And Burst Capture

Implemented:

- `monitor-event-live`;
- baseline tick capture;
- cloud semantic match trigger;
- high-confidence fresh-post burst trigger;
- burst deduplication through `live_burst_runs`;
- bounded burst phases:
  - `1s x 60s`;
  - `10s x 10m`;
  - `60s x 110m`;
- live report update after burst.

Current live burst record:

| Case | Trigger Post | Account | Confidence | Status | Planned Calls | Ticks Written |
| --- | --- | --- | ---: | --- | ---: | ---: |
| `live_us_iran_permanent_peace_may31` | `2053453168739008922` | `ELINTNews` | 0.98 | `completed` | 230 | 230 |

Important issue discovered and fixed:

- The first live burst was technically valid data collection, but it was triggered by an older post that had just been newly matched by the cloud model.
- A freshness guard was added so old backfill posts can still be used for research but cannot trigger live burst.

### 6.8 Paper-Trade Proxy

Implemented:

- execution-cost adjustment in micro backtest;
- `execution_cost`;
- `net_close_delta`;
- `net_max_favorable_delta`;
- `net_max_adverse_delta`;
- `paper_trade_positive`;
- `paper_trade_strong`;
- `cost_erased_move` risk tag;
- `high_execution_cost` risk tag;
- reports now show gross vs net move.

Current May31 paper-trade summary:

| Metric | Count |
| --- | ---: |
| micro impacts | 1,856 |
| paper-trade positive | 14 |
| tradable after freshness/entry-delay/cost filters | 0 |
| cost-erased moves | 18 |

Important interpretation:

- Some raw price moves look positive before cost.
- Once conservative execution cost is applied, almost none are currently tradable.
- This is exactly the type of protection needed before discussing any execution layer.

Current limitation:

- many live ticks are midpoint-only.
- without bid/ask, the execution cost model uses a conservative fallback.
- next development should capture bid/ask/spread during burst.

### 6.9 Future Execution Layer

Not implemented yet.

Intended future role:

- consume validated signals from the research OS;
- connect to prediction-market trading APIs;
- allocate capital across dedicated wallets;
- enforce risk constraints before any order is submitted;
- record all order attempts, fills, cancels, and failures.

The execution layer should not be a small extension inside signal scoring. It should be a separate subsystem with its own configuration, tests, logs, and kill switch.

Expected execution config:

- venue;
- API credential environment variable names;
- wallet label;
- wallet address;
- max capital per wallet;
- max exposure per market;
- max exposure per event family;
- max daily loss;
- allowed order types;
- live/dry-run mode;
- manual approval requirement.

Minimum gates before live order submission:

- signal is fresh;
- semantic match confidence is high enough;
- market liquidity and spread are acceptable;
- cost-adjusted paper-trade proxy is positive;
- no duplicate order was already created for the same signal;
- wallet exposure remains under limit;
- global exposure remains under limit;
- kill switch is off.

### 6.10 Signal Source Discovery Automation

Implemented:

- new `discover-signal-sources` CLI command;
- scans active, unfinished Polymarket markets through Gamma;
- ranks high-interest markets by liquidity, volume, deadline, and narrative terms;
- default focus is `narrative`, which excludes sports pools unless explicitly requested;
- performs bounded X recent search per selected market;
- optionally pulls public Reddit search results as low-confidence discussion context;
- stores discovered X posts and source candidates locally;
- writes local discovery reports under `data/reports/`;
- supports `--dry-run` budget planning before X API calls.

Created Codex automation:

- name: `Signal Source Discovery`;
- id: `signal-source-discovery`;
- frequency: every 6 hours;
- workspace: `/Users/jamesli/Desktop/Sol Projects/quant_sol`;
- default command:

```bash
python3 -m quant_sol.signals discover-signal-sources \
  --max-markets 8 \
  --max-gamma-pages 2 \
  --min-liquidity 100000 \
  --focus narrative \
  --lookback 24h \
  --max-posts-per-market 20 \
  --daily-cap 200 \
  --include-reddit \
  --reddit-limit 5
```

Safety constraints:

- does not collect follow graph;
- does not call trading APIs;
- does not touch private keys or wallet signing;
- writes only local data and reports;
- X API usage is bounded by selected market count.

First manual run:

- markets scanned: 5;
- X planned calls: 5;
- report path: `data/reports/signal_source_discovery_20260516T155320Z.md`;
- result quality note: raw X keyword search produces noisy single-post accounts, so promotion threshold was tightened. A source now needs enough score and either repeated posts or meaningful engagement before being marked `candidate_signal_source`.

## 7. Reports Generated Locally

Current local reports include:

- account ranking report;
- Annica pattern deep dive;
- Trump China visit event backtest;
- China/Taiwan event backtests;
- US-Iran nuclear deal report;
- US-Iran permanent peace deal May31 / June30 / Dec31 reports;
- model diagnostics reports;
- political thesis / large capital report;
- sports odds / latency report;
- interval market / realized-only bias report;
- wallet reports;
- watchlist candidate report.

These files live under `data/reports/` and should remain local.

## 8. Test Status

Current test inventory:

- 60 tests collected.

Recent full test result:

```text
60 passed, 1 warning
```

The warning is the local Python `urllib3` / LibreSSL warning. It is noisy but not currently blocking.

Covered test areas:

- CLI command exposure;
- Polymarket client pagination;
- wallet config and storage;
- X API env and caps;
- event history and micro backtest;
- semantic matching;
- cloud matching failure modes;
- model diagnostics;
- Telegram formatting;
- signal scoring;
- Web3 account ranking.

## 9. Current Git / Commit State

Current working tree has uncommitted changes.

Modified tracked files:

- `.gitignore`
- `config/api_limits.yaml`
- `pyproject.toml`
- `quant_sol/signals/cli.py`
- `quant_sol/signals/config.py`
- `quant_sol/signals/history.py`
- `quant_sol/signals/storage.py`
- `tests/test_event_history.py`
- `tests/test_model_diagnostics.py`

Untracked files:

- `goal/`
- `progress/`
- `quant_sol/signals/semantic.py`

Important:

- do not commit `.env`;
- do not commit `data/`;
- do not commit `config/semantic_matching.yaml`;
- do not commit DuckDB files or raw API responses.

The current code is functionally advanced compared with the last commit, but still needs a careful commit boundary because many changes are bundled together:

1. semantic matching;
2. micro price-in backtest;
3. live burst monitor;
4. X API cap update;
5. paper-trade proxy;
6. long-term goal and progress docs.

## 10. Main Technical Gaps

### 10.1 Burst Needs Real Bid/Ask

The biggest current modeling gap is execution quality.

Current issue:

- burst ticks are mostly midpoint-based;
- spread is often missing;
- cost model falls back to conservative assumptions;
- this makes paper-trade evaluation safer but less precise.

Next fix:

- during burst, fetch orderbook or best bid/ask alongside midpoint;
- store `best_bid`, `best_ask`, `spread`, `liquidity`;
- use actual spread for net move.

### 10.2 More Valid Live Samples Needed

One burst run is not enough.

Target:

- at least 20 valid live burst runs;
- preferably across multiple event families;
- enough samples per account before ranking them as useful or noisy.

### 10.3 Source Ranking Needs Live Evidence

Current account ranking is useful, but confidence is limited.

Need:

- more live post-to-price samples;
- distinction between early source, fast curator, and late amplifier;
- source-chain discovery for upstream accounts;
- reliable penalty for accounts that post after price already moved.

### 10.4 Wallet Flow Is Not Yet Integrated

Wallet collector exists, but the main event-signal loop is still mostly X plus market price.

Need:

- fill `wallet_activity` for watchlist wallets;
- map wallet activity to event cases;
- detect large same-direction entries around narrative acceleration;
- include wallet flow only as supporting evidence, not as the sole trigger.

### 10.5 Market Selection Should Become Scored

Currently the user and model choose cases manually.

Need:

- market selection score using liquidity, spread, maturity, volatility, and narrative sensitivity;
- multi-maturity grouping;
- automatic rejection of thin, near-deadline, or already-crowded markets unless explicitly studying overreaction.

### 10.6 Execution Is Not Designed Yet

The long-term goal may include connecting to prediction-market trading APIs and assigning wallets, but this is not implemented.

Missing pieces:

- execution package boundary;
- venue API abstraction;
- wallet allocation config;
- risk engine;
- paper-trading ledger;
- order idempotency;
- kill switch;
- audit log;
- tests for every risk check.

This should not be added until live micro evidence and paper-trade results are more convincing.

## 11. Recommended Next Development Rounds

### Round 1: Burst Bid/Ask Upgrade

Goal:

- make paper-trade cost less approximate.

Implementation:

- update burst collection to fetch best bid/ask if available;
- store `best_bid`, `best_ask`, `spread`;
- add tests where actual spread changes net tradability;
- rerun May31 report.

Acceptance:

- reports show actual spread/cost where available;
- `execution_cost` is not only fallback;
- `paper_trade_positive` becomes more reliable.

### Round 2: Live Monitor Operational Loop

Goal:

- collect valid live samples without manual babysitting.

Implementation:

- run `monitor-event-live` with bounded iterations during active news windows;
- log skipped stale triggers;
- log triggers rejected by low confidence or old post age;
- optionally add Telegram notice for "burst started" and "burst completed."

Acceptance:

- several live burst runs completed;
- no duplicate bursts for same post/case;
- old backfill posts never trigger.

### Round 3: Source Expansion

Goal:

- improve the account universe beyond current seeds.

Implementation:

- add more independent reporters, regional journalists, and OSINT accounts;
- sync small batches under daily cap;
- rank by live evidence, not engagement.

Acceptance:

- accounts are tiered by measured price-path usefulness;
- noisy/late accounts are clearly labeled.

### Round 4: Wallet Flow Integration

Goal:

- add wallet flow as supporting evidence.

Implementation:

- sync political/geopolitical wallet watchlist;
- map activities to event cases;
- add window features around posts:
  - pre-post 10m;
  - post 10m;
  - post 1h;
- include wallet-flow evidence in reports.

Acceptance:

- reports show whether watched wallets moved before, during, or after narrative bursts.

### Round 5: Market Selection Engine

Goal:

- stop choosing markets purely manually.

Implementation:

- score active markets by liquidity, spread, end time, volatility, tags, and semantic relation to watched narratives;
- output top candidates for live monitoring.

Acceptance:

- CLI can recommend live cases;
- chosen markets have clear research reason.

### Round 6: Execution Architecture Design

Goal:

- prepare the project for eventual API-based execution without compromising the current read-only research system.

Implementation:

- create an execution design doc;
- define wallet allocation schema;
- define order intent schema;
- define risk-check interface;
- define paper-trade ledger;
- define dry-run command shape;
- explicitly document what must be true before live trading is enabled.

Acceptance:

- execution requirements are documented;
- wallet allocation and risk model are testable without real credentials;
- no live API calls are made;
- research and execution code paths remain separate.

### Round 7: Paper Execution Simulator

Goal:

- simulate trading behavior using real signal timestamps and market state.

Implementation:

- convert validated signals into order intents;
- allocate notional to a paper wallet;
- apply spread, slippage, and liquidity assumptions;
- record entry, exit, PnL, drawdown, and reason for rejection;
- compare signal score to paper PnL.

Acceptance:

- every paper order has a traceable signal id;
- rejected orders show which risk rule blocked them;
- account/source rankings can include simulated execution quality.

### Round 8: Optional Live Execution Adapter

Goal:

- only after paper execution is stable, add a narrowly scoped live adapter for one prediction-market venue.

Requirements:

- explicit user approval;
- dedicated wallet only;
- small capped allocation;
- dry-run mode remains default;
- kill switch is tested;
- duplicate order protection is tested;
- all live actions are logged locally.

## 12. Useful Current Commands

Check API status:

```bash
python3 -m quant_sol.signals check-api
```

Run bounded live monitor:

```bash
python3 -m quant_sol.signals monitor-event-live \
  --cases live_us_iran_permanent_peace_may31,live_us_iran_permanent_peace_june30,live_us_iran_permanent_peace_dec31 \
  --iterations 1 \
  --daily-x-cap 200
```

Run micro backtest:

```bash
python3 -m quant_sol.signals run-event-backtest \
  --case live_us_iran_permanent_peace_may31 \
  --mode micro
```

Write event report:

```bash
python3 -m quant_sol.signals report-event-backtest \
  --case live_us_iran_permanent_peace_may31
```

Run diagnostics:

```bash
python3 -m quant_sol.signals diagnose-model
```

Run tests:

```bash
python3 -m pytest
```

## 13. Practical Interpretation Of Current Research Result

The project has now reached a useful research state:

- content ingestion works;
- semantic matching works;
- historical event backtests work;
- micro windows are modeled correctly;
- stale backfill posts are blocked from live burst triggers;
- live burst data collection is proven possible;
- reports now separate gross price movement from cost-adjusted paper-trade movement.

But the current evidence is not enough to claim a tradable edge.

The strongest current conclusion is methodological:

> The system can now prevent several common false positives: stale posts, minute-floor data, already-moved markets, slow entry ticks, and cost-erased price moves.

The next meaningful milestone is not adding more scoring complexity. It is collecting more valid live burst samples with real bid/ask/spread.
