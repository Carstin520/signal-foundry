# Prediction Market Feasibility And Wallet Tracking Plan

Date: 2026-05-14

## Working Conclusion

Prediction-market arbitrage should not be treated as the first live-capital strategy. It is still useful as a data-engineering and market-microstructure exercise, but current evidence suggests that most clean arbitrage is too small, too short-lived, or too fee/slippage constrained to scale.

The more promising research track is a public-wallet intelligence layer:

1. Collect public Polymarket wallet activity.
2. Normalize realized/unrealized PnL by market category.
3. Detect unusually timed, unusually sized, high-conviction entries.
4. Separate repeatable strategy from one-off luck, stale leaderboard artifacts, wash-like behavior, and survivorship bias.

This should be framed as public market-flow research, not as proof of illegal insider trading. A wallet can look suspicious without any direct evidence of non-public information.

## Arbitrage Feasibility Notes

Important constraints:

- Polymarket market data is public and does not require authentication.
- Trading fees apply to many market categories. Taker fee formula is `shares * feeRate * price * (1 - price)`.
- Makers are not charged fees, but maker execution is not guaranteed.
- Kalshi orderbooks expose only bids; asks are implied from the opposite-side bid.
- Gas is less important than expected on Polymarket if using relayed/proxy flows, but execution loss still comes from spread, taker fees, depth, stale books, matching latency, settlement timing, and capital transfer/withdrawal friction.

Empirical read:

- Single-market Polymarket NBA arbitrage was found to be extremely rare and short-lived.
- Combinatorial opportunities existed but were mostly constrained by shallow depth.
- A 1% displayed edge can disappear after taker fees and crossing multiple books.

Therefore the first scanner should report opportunities, but should not assume they are executable until it models:

- full depth, not top of book;
- fee per leg;
- minimum tick;
- stale snapshot age;
- partial-fill risk;
- settlement/withdrawal timing;
- whether all legs can be crossed atomically or near-atomically.

## Initial Public Wallet Watchlist

These are not endorsements. They are starting points found from X posts, public profiles, and third-party analytics pages, and each needs API-level verification.

| Label | Address / Profile | Why Watch | Initial Read |
| --- | --- | --- | --- |
| aviato | `0x2a019dc0089ea8c6edbbafc8a7cc9ba77b4b6397` | X/Polyrating cited high win rate, high volume, stake-weighted performance. | Struct shows active since Jul 2024, high volume, but PnL differs from X/Polyrating numbers. Good candidate for metric reconciliation. |
| Annica | `0x689ae12e11aa489adb3605afd8f39040ff52779e` | Cited on X as strong Musk/tweet-related trader. | Struct shows very high volume and PnL, but current open-position list includes many small losing tail positions. Need category-level and realized-only analysis. |
| reachingthesky | `0xefbc5fec8d7b0acdc8911bdd9a98d6964308f9a2` | Viral March 2026 soccer whale. | Reported 33% win rate but very large positive PnL from asymmetric sizing. Likely not a high-win-rate wallet; useful for sizing/asymmetry study. |
| GCottrell93 | `0x94a428cfa4f84b264e01f70d93d02bc96cb36356` | High-profile political/geopolitical bettor. | Reported large lifetime PnL but also large single-cluster loss. Useful example of why "access" and "edge" do not remove tail risk. |
| majorexploiter | profile `@majorexploiter`, partial address `0x0197...9f3c` | Public reports describe very new wallet, few trades, large sports profit, and suspicious timing. | High suspicion signal, but address needs full API/profile extraction before adding to automated monitoring. |

## Verification Method

For each wallet:

1. Pull current and closed positions from Polymarket Data API.
2. Pull public activity and trade fills if available.
3. Recompute:
   - realized PnL;
   - unrealized PnL;
   - total volume;
   - number of markets;
   - category distribution;
   - max single-trade contribution to PnL;
   - recent 30/90-day decay;
   - average entry time relative to market close and major public news timestamps.
4. Flag suspicious patterns:
   - brand-new wallet with large funding and few large bets;
   - category jump before major event;
   - entries before public event timestamp;
   - very high concentration in one event cluster;
   - multiple funded wallets converging on same side;
   - realized PnL inflated by not redeeming losers or leaving losses unrealized.

## API Setup

Read-only Polymarket endpoints:

```bash
curl "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=5"
curl "https://data-api.polymarket.com/positions?user=0x..."
curl "https://data-api.polymarket.com/closed-positions?user=0x..."
curl "https://data-api.polymarket.com/activity?user=0x..."
curl "https://data-api.polymarket.com/value?user=0x..."
```

Kalshi orderbook endpoint:

```bash
curl "https://external-api.kalshi.com/trade-api/v2/markets/{ticker}/orderbook"
```

Polymarket trading requires wallet/API credentials. Do not enable this in the project until read-only analytics is reliable. Never paste a main-wallet private key into third-party copy-trading code.

## Proposed Next Iterations

Round 1:
- Build a read-only Polymarket wallet collector for the five watchlist wallets.
- Store raw JSON snapshots in `data/raw/`.
- Store normalized wallet/position/trade tables in DuckDB.

Round 2:
- Implement wallet scoring:
  - realized PnL quality;
  - concentration risk;
  - recency;
  - category specialization;
  - suspicious timing score.

Round 3:
- Build a market-event timeline module:
  - manually curated public timestamp;
  - price before/after;
  - wallet entry before/after;
  - signal half-life.

Round 4:
- Add alerts for:
  - watched wallet opens new position above threshold;
  - several high-score wallets converge on same side;
  - brand-new wallet places unusually large bet in illiquid market.

