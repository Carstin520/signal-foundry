# Signal Foundry Long-Term Research Goal

Date: 2026-05-16

## 1. Core Mission

Signal Foundry is currently a read-only prediction-market research system, but its long-term scope is broader: build a disciplined pipeline for finding, measuring, validating, and eventually executing information edges in prediction markets. The first major research target is the short-lived gap between public narrative formation and market price adjustment.

The project should not be optimized for "front-running confirmed news." Once an official, settlement-relevant fact is public, the market is often already in a latency race against faster participants, market makers, and possibly better-informed actors. The more valuable research target is earlier and less certain:

- public narratives begin to heat up;
- high-timeliness accounts start discussing the same theme;
- the market has not fully moved yet;
- liquidity is sufficient to enter and exit;
- the price then climbs, dumps, or oscillates as participants FOMO into the story.

The system should first answer one practical question:

> When a narrative becomes tradable before it becomes certain, can we identify the accounts, markets, and time windows where Polymarket price-in behavior is slow enough to exploit?

If the answer becomes consistently positive after realistic cost and risk adjustment, the long-term system may expand into a controlled execution layer:

- connect to prediction-market trading APIs;
- allocate capital across dedicated strategy wallets;
- enforce per-wallet, per-market, and per-day risk limits;
- keep the research OS, signal generation, and execution services separated;
- require explicit authorization before any live trading mode is enabled.

## 2. Strategic Thesis

The working thesis is that prediction-market prices do not always adjust instantly to non-official but high-signal public information. The exploitable edge, if it exists, is not necessarily final resolution accuracy. It is the intermediate price path.

The target opportunity is a FOMO premium:

1. A market has a live narrative with meaningful uncertainty.
2. Relevant posts appear from fast, high-signal sources.
3. The orderbook still shows inertia.
4. The market later reprices over seconds to minutes as more participants discover the narrative.
5. The trade can be exited before final event risk dominates.

This means success should be measured by short-horizon market movement, not only by whether the market eventually resolves Yes or No.

Primary evaluation windows:

- `1s`
- `10s`
- `30s`
- `1m`
- `5m`
- `10m`
- `30m`
- `2h`

The most important windows are `1m`, `5m`, and `10m`. Anything after `2h` is mainly context; by then much of the emotional repricing may already be complete.

## 3. What This Project Is Not

Signal Foundry should avoid drifting into unclear or unsafe objectives.

In its current stage, it is not:

- an auto-trading bot;
- a private-key or trading-API integration;
- an insider-trading detector making legal claims;
- a system for scraping private data, DMs, cookies, or non-public information;
- a model that blindly trusts X engagement or influencer screenshots;
- a final-resolution prediction engine only;
- a generic Web3 news dashboard.

In a later stage, the project may include controlled execution, but only after paper-trade evidence supports it. Execution should be treated as a separate, auditable subsystem, not as an implicit side effect of signal generation.

The system can study suspicious wallet timing, high-conviction entries, and unusual flows, but it should describe them as public-data patterns rather than proof of insider information.

## 4. Research Objects

The project should focus on four research objects.

### 4.1 Markets

Preferred markets:

- high-liquidity prediction markets;
- markets with several related maturities or submarkets;
- markets driven by narrative uncertainty rather than single official confirmation;
- markets where price can move meaningfully before final resolution;
- markets with enough time left that participants can still FOMO.

Examples:

- US-Iran permanent peace deal by specific dates;
- US-Iran nuclear deal before a deadline;
- China/Taiwan escalation markets;
- election, nomination, resignation, leadership, tariff, sanction, regulation markets;
- high-volume geopolitical markets with repeated news waves.

Lower priority markets:

- very near-deadline fact markets;
- "by tomorrow" or "by Friday" binary news markets;
- speech word markets;
- very thin markets;
- markets already above crowded probability zones unless studying overreaction;
- markets where the resolution source itself is the only relevant information.

### 4.2 Sources

Web3 accounts are useful because many of them are fast at detecting market-moving narratives, but the project is not only about Web3 markets. The role of Web3 accounts is timeliness, not topic restriction.

Source categories:

- independent reporters;
- regional journalists;
- OSINT accounts;
- fast curators;
- market translators;
- Web3-native high-speed information accounts;
- official confirmation sources;
- noisy but high-frequency volatility context accounts.

The ranking goal is to identify:

- who posts earliest;
- who posts before price movement;
- who causes or precedes discussion cascades;
- who is merely reacting after the price has moved;
- who is fast but noisy;
- who is slower but higher quality;
- whose source chain points to better upstream accounts.

Official accounts should usually be treated as confirmation or exclusion sources, not alpha sources.

### 4.3 Wallets

Wallet research should be used as a supporting signal, not as the only trigger.

Useful wallet questions:

- Does a watched wallet enter before or after public narrative acceleration?
- Is the entry size abnormal compared with its own history?
- Is the wallet concentrated in one event family?
- Is realized PnL hiding large open losses?
- Does the wallet repeatedly appear before price movement in a specific category?

Wallet signals should be marked with confidence and risk tags. A wallet can have good timing without the project claiming why.

### 4.4 Price Path

The central object is the price path after a candidate information event.

Important metrics:

- entry delay from post time to nearest tradable tick;
- `1s`, `10s`, `30s`, `1m`, `5m`, `10m` move;
- time to 3 percentage point movement;
- maximum favorable move within 10 minutes;
- maximum adverse move within 10 minutes;
- reversal by 30 minutes;
- whether price moved before the post;
- whether spread/liquidity made the move executable.

Historical minute-level data is not enough to validate second-level edge. It can support broad context, but true micro price-in evidence requires live burst or WebSocket capture.

## 5. System Architecture Target

The long-term system should have five layers.

### 5.1 Data Collection Layer

Inputs:

- Polymarket Gamma API for market metadata;
- Polymarket CLOB/public endpoints for price ticks and midpoint history;
- Polymarket Data API for wallet activity;
- X API for public posts, timelines, search, and source discovery;
- optional CSV/manual imports when API limits block full automation;
- Telegram for alert output.

Rules:

- all raw data stays under `data/`;
- `.env`, DuckDB, raw API responses, and local reports are not committed;
- API usage is capped locally;
- no trading credentials or private keys are introduced in the research layer.

Future execution inputs, if enabled later:

- prediction-market trading API credentials;
- dedicated strategy wallet addresses;
- wallet funding limits;
- venue-specific order constraints;
- execution logs and audit trails;
- kill-switch and manual approval state.

### 5.2 Matching Layer

The matching layer connects posts, markets, wallets, and event families.

V1 uses:

- keyword anchors for precision;
- cloud or local semantic matching for recall;
- rule-based direction classification;
- manual case configuration for important markets.

Long-term target:

- semantic matching should find indirect but relevant posts;
- direction should remain auditable;
- the system should separate "related context" from "actionable directional signal";
- every match should have evidence, confidence, and rejection reasons.

### 5.3 Live Capture Layer

The live layer is the key upgrade over historical analysis.

Baseline mode:

- collect low-frequency ticks for watched markets;
- keep enough background data to know whether a market was already moving.

Burst mode:

- triggered only by fresh high-confidence matches;
- collect `1s x 60s`;
- then `10s x 10m`;
- then `60s x 110m`;
- write every run to `live_burst_runs` for audit and deduplication.

The burst trigger must reject stale backfill posts. Old posts can be used for research, but not for live micro evidence.

### 5.4 Backtest Layer

The backtest layer should answer whether a source or signal would have been useful if treated as an ideal entry time.

Core tests:

- Did price move before the post?
- Did price move after the post?
- How fast did it move?
- Was the move large enough to beat spread and slippage?
- Did it reverse quickly?
- Is the result repeated across events or just one lucky sample?

Ranking should penalize:

- late-after-price-move behavior;
- low sample size;
- one-account spam;
- no liquidity;
- wide spreads;
- crowded prices;
- stale official confirmations;
- data resolution that is too coarse.

### 5.5 Reporting And Alerting Layer

Reports should be written for research decisions, not for hype.

Every report should make clear:

- which case was studied;
- which market and maturity were used;
- which posts triggered matches;
- whether price data was live or historical;
- whether sub-minute conclusions are valid;
- which accounts looked early;
- which accounts looked late;
- which signals were false FOMO;
- what should be changed in the next run.

Telegram alerts should be short and explainable:

- market;
- source post;
- narrative direction;
- current probability;
- spread/liquidity;
- confidence;
- trigger age;
- recent price movement;
- risk tags.

### 5.6 Future Execution Layer

The execution layer is a future phase, not part of the current research loop. It should only be built after the research system demonstrates repeatable, cost-adjusted edge.

Execution responsibilities:

- submit orders through supported prediction-market APIs;
- allocate trades across dedicated wallets;
- enforce risk budgets before order submission;
- record every signal, decision, order, fill, cancel, and error;
- prevent the same signal from over-allocating across multiple markets or maturities;
- expose a hard kill switch.

Wallet allocation principles:

- never use a personal main wallet;
- use separate wallets by strategy, market family, or risk bucket;
- cap capital per wallet;
- cap capital per event family;
- cap loss per day and per burst window;
- reserve one wallet for paper/sandbox testing if the venue supports it;
- treat funding and withdrawal operations as manual until the system is mature.

Execution gating:

- signal must pass freshness checks;
- market must pass liquidity and spread checks;
- paper-trade proxy must remain positive after cost assumptions;
- entry delay must be within the configured window;
- the market must not be stale, resolved, suspended, or too close to deadline unless explicitly allowed;
- the final order must pass per-wallet and global exposure checks.

The first execution implementation, if pursued, should support simulation and dry-run modes before live orders.

## 6. Research Principles

### 6.1 Prefer Tradability Over Story Quality

A story can be correct but untradable. A signal matters only if the market price path leaves enough room after spread, slippage, and reaction delay.

### 6.2 Prefer Repeatability Over Anecdotes

A single excellent call is not enough. The system should identify repeated behavior by account, market family, and time window.

### 6.3 Separate Speed From Accuracy

Some accounts are fast but noisy. Some are accurate but late. Both can be useful, but they should not be ranked by the same score.

### 6.4 Treat Final Resolution As Secondary

For FOMO premium research, the important question is often whether the market moved in the next 1 to 10 minutes, not whether the event eventually happened.

### 6.5 Avoid False Precision

If only minute-level data exists, the report must not pretend to know 1s or 10s behavior. If a post is historical backfill, it must not trigger live burst logic.

### 6.6 Keep Research And Execution Separated

The research system should remain read-only until the data proves a stable, executable edge. Even then, any trading layer should be designed as a separate module with explicit risk controls, dedicated wallets, execution logs, and kill-switch behavior.

Signal generation should never directly imply order submission. The future execution path should be:

1. signal;
2. validation;
3. cost and liquidity check;
4. wallet allocation check;
5. dry-run order preview;
6. live order only when explicitly enabled.

## 7. Success Metrics

The project should be judged by research quality and signal validation.

Market-level metrics:

- number of high-liquidity markets under watch;
- tick coverage by market;
- live burst coverage after fresh signals;
- spread/liquidity at signal time;
- price move distribution by horizon.

Post-level metrics:

- match confidence;
- direction confidence;
- entry delay;
- 1m, 5m, 10m move;
- max favorable and adverse move;
- reversal rate;
- whether price moved before the post.

Account-level metrics:

- lead score;
- micro hit rate;
- sub-10m hit rate;
- median time to price-in;
- late-after-price-move rate;
- false FOMO rate;
- source chain score;
- sample size.

System-level metrics:

- API calls used per useful signal;
- false trigger rate;
- burst runs completed;
- reports generated;
- tests passing;
- data quality warnings.

Future execution metrics:

- paper-trade expectancy after costs;
- live-vs-paper slippage;
- fill rate;
- cancel rate;
- wallet utilization;
- max drawdown per wallet;
- exposure by market family;
- realized PnL by signal type;
- error and rejected-order rate;
- kill-switch activations.

## 8. Roadmap

### Phase 1: Stable Research Base

Goal: make the current system reliable.

Tasks:

- keep DuckDB schema stable;
- protect secrets and raw data with `.gitignore`;
- maintain tests for wallet, market, semantic, and micro backtest logic;
- keep X API daily cap configurable;
- keep reports reproducible from local data.

Definition of done:

- full test suite passes;
- no secrets or raw data are tracked;
- current commands can be run without breaking DB state.

### Phase 2: Live Micro Evidence

Goal: gather enough real second-level samples.

Tasks:

- run baseline monitoring on selected high-liquidity markets;
- trigger burst only on fresh high-confidence posts;
- compare live burst result with historical minute-floor result;
- collect enough samples per case and account.

Definition of done:

- at least 20 valid live burst runs;
- each run has `1s/10s/30s/1m/5m/10m` evidence;
- stale backfill triggers are blocked;
- report clearly separates valid micro evidence from insufficient data.

### Phase 3: Source Ranking

Goal: identify which accounts are genuinely useful.

Tasks:

- expand independent journalist and OSINT watchlists;
- rank accounts by speed, cascade, and price-path impact;
- detect upstream sources followed or quoted by high-score accounts;
- distinguish fast curators from original sources.

Definition of done:

- each account has enough samples for confidence labeling;
- source tiers are updated from measured performance, not intuition;
- late followers and noisy accounts are downgraded automatically.

### Phase 4: Market Selection Engine

Goal: choose markets with the best FOMO premium conditions.

Tasks:

- rank markets by liquidity, spread, maturity, volatility, and narrative sensitivity;
- prefer multi-maturity event families;
- reject near-deadline or already-crowded markets unless explicitly studying overreaction;
- build case templates for geopolitics, elections, tariffs, sanctions, and crypto policy.

Definition of done:

- new cases can be added quickly;
- market choice is scored, not ad hoc;
- reports compare multiple maturities of the same narrative.

### Phase 5: Paper Trading Simulator

Goal: estimate whether the signal survives realistic execution assumptions.

Tasks:

- simulate entry at next available tick after post time;
- include spread, slippage, and liquidity constraints;
- evaluate exits at 1m, 5m, 10m, and trailing stop rules;
- measure drawdown and reversal risk.

Definition of done:

- signals are evaluated by executable PnL proxy, not just price movement;
- strategy variants can be compared without live capital;
- bad signals are filtered before any trading discussion.

### Phase 6: Controlled Execution Research

Goal: only after strong evidence, design a separate execution layer that can connect to prediction-market trading APIs and allocate capital across dedicated wallets.

Requirements before this phase:

- enough live samples;
- stable positive expectancy after costs;
- reliable signal freshness;
- clear risk controls;
- explicit user approval.

Execution-layer tasks:

- design a separate execution package or service;
- define wallet allocation config;
- support dry-run order preview;
- support paper-trading ledger;
- connect to prediction-market trading APIs only after dry-run behavior is stable;
- implement per-wallet and global exposure limits;
- implement order idempotency and duplicate-signal protection;
- implement kill switch and manual disable mode;
- write execution audit logs to local storage.

Definition of done:

- no signal can submit an order without passing risk checks;
- every live-capital wallet has an explicit allocation limit;
- every order can be traced back to signal evidence and market state;
- paper mode and live mode are clearly separated;
- tests cover wallet allocation, exposure caps, duplicate signals, and kill switch behavior.

This phase should remain separate from the research OS. The research layer may produce candidate signals; the execution layer decides whether an order is allowed.

## 9. Current Priority

The immediate priority is not to add more features. It is to collect better live evidence.

Highest value next actions:

1. Run live baseline and burst monitoring during active geopolitical news windows.
2. Expand high-timeliness independent journalist accounts.
3. Focus on high-liquidity, multi-maturity markets such as US-Iran peace/nuclear deal cases.
4. Keep rejecting stale backfill posts as live triggers.
5. Evaluate every burst by 1m, 5m, and 10m movement.
6. Build account rankings only from valid live or sufficiently dense tick data.

## 10. Long-Term North Star

The long-term north star is a system that can say:

> This account or source cluster tends to surface a specific type of narrative before the prediction market fully prices it in. In these market conditions, the average price-in path leaves a measurable, short-lived, executable window. In these other conditions, the signal is too late, too noisy, too illiquid, or already priced in.

The broader north star adds one more requirement:

> When the edge is validated, the system can allocate a controlled amount of capital through dedicated wallets, execute only under explicit risk rules, and produce a full audit trail from source post to signal to order to outcome.

If the system can answer the research question with local data, reproducible reports, conservative assumptions, and eventually controlled execution records, it has achieved its main purpose.
