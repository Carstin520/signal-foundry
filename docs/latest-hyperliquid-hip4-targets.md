# Latest Hyperliquid HIP-4 Targets

This file is overwritten by the combined prediction-market discovery heartbeat.

- Last updated: 2026-05-19T08:50:30+00:00
- Markets scanned: 6
- Venue: Hyperliquid HIP-4 public info endpoint
- Venue role: high_frequency_outcome_clob

## Selection Rules

- Prefer outcome markets with observable allMids and l2Book depth.
- Treat priceBinary and priceBucket outcomes as digital-option-like markets, not simple social probabilities.
- Use perp/spot hedge context only as an inferred research lens until explicitly validated.
- Do not use trading APIs, private keys, wallet signatures, or order endpoints.

## Current Targets

| Rank | Coin | Side | Category | Score | Mid | Bid | Ask | Spread | Depth | Descriptor |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `#651` | No | crypto_outcome | 81.062 | 0.378 | 0.375 | 0.383 | 0.008 | 1598.000 | class:priceBinary,underlying:BTC,expiry:20260520-0600,targetPrice:76886,period:1d |
| 2 | `#650` | Yes | crypto_outcome | 80.390 | 0.622 | 0.616 | 0.625 | 0.009 | 1598.000 | class:priceBinary,underlying:BTC,expiry:20260520-0600,targetPrice:76886,period:1d |
| 3 | `#690` | Yes | other | 60.864 | 0.139 | 0.136 | 0.142 | 0.006 | 1118.000 | index:2 |
| 4 | `#691` | No | other | 60.864 | 0.861 | 0.858 | 0.865 | 0.006 | 1118.000 | index:2 |
| 5 | `#670` | Yes | other | 53.770 | 0.073 | 0.068 | 0.079 | 0.011 | 525.000 | index:0 |
| 6 | `#671` | No | other | 53.770 | 0.927 | 0.921 | 0.932 | 0.011 | 525.000 | index:0 |

## Model Discipline

- Provenance: outcome metadata, mids, and orderbook are observed public Hyperliquid data; hedge context is inferred.
- Edge class: liquidity/latency or model-relative unless a narrative market is explicitly mapped.
- Participant lens: retail sees bounded-loss outcomes, institutions see hedge/capacity context, market makers see inventory plus hedge-basis risk.
- Tradability remains unvalidated until spread, depth, expiry, settlement, and hedge-basis checks pass.