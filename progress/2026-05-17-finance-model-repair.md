# Finance Model Repair Notes

Date: 2026-05-17

## Purpose

This repair pass tightens the micro price-in model from a market-move detector into a more finance-aware research signal. The goal is to avoid ranking accounts or posts as useful when the observed move is too small after spread/slippage, too path-dependent, or based on too few samples.

## Changes Made

### Cost-Adjusted Edge

Micro post impacts now store explicit cost-adjusted fields:

- `edge_after_cost`
- `reward_to_risk`
- `risk_adjusted_edge`

The model still records gross favorable moves, but a post is not treated as tradable unless the move remains meaningful after estimated round-trip spread and slippage.

### Path Risk Guard

The model now penalizes signals with large adverse excursion even if the final or maximum favorable move looks good. New risk tags:

- `poor_reward_to_risk`
- `adverse_excursion`
- `thin_risk_adjusted_edge`

This is important for prediction-market FOMO signals because a large early drawdown can make the signal practically unusable even when the later chart looks correct.

### Tradability Gate

A micro signal must now pass all of these gates before `tradable_ramp=True`:

- gross move reaches the micro threshold;
- net max favorable move survives estimated costs;
- reward/risk is at least `1.5`;
- max adverse move is better than `-4pp`;
- risk-adjusted edge is at least `1.5pp`;
- entry tick is not slow;
- the market was not already moving before the post.

### Sample-Size Shrinkage

Account ranking now applies a sample-confidence adjustment. A single good post can be marked as interesting, but it no longer promotes the account to a high-confidence micro source.

New behavior:

- one or two cost-positive samples: `needs_more_samples`;
- enough repeatable, sub-10m, cost-positive samples: `micro_source`;
- gross moves erased by costs: `cost_erased_watch`;
- minute-level-only data remains `insufficient_resolution`.

## Why This Matters

The previous model could overstate edge in three common cases:

1. The market moved 3pp, but the spread/slippage consumed the move.
2. The market moved favorably later, but first moved too far against the ideal entry.
3. One account had one good historical sample and was ranked too aggressively.

The updated scoring is stricter and should produce fewer false positives.

## Remaining Gaps

These changes still do not prove live profitability. The system still needs:

- more live burst samples;
- realistic order book depth and partial-fill modeling;
- explicit exit rules;
- paper-trade ledger by signal family;
- account ranking across multiple market families;
- execution-layer separation before any live trading work.

## Verification

Current test result:

```text
65 passed
```

