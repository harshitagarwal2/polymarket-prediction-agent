# Sports Benchmark Suite Summary

## Aggregate metrics

- Total cases: 3
- Successful cases: 3
- Failed cases: 0
- Fair-value cases: 2
- Replay cases: 2
- Average Brier score: 0.212168
- Average log loss: 0.616977
- Average accuracy: 1.000000
- Average ECE: 0.000000
- Calibrated fair-value cases: 0
- Average calibrated Brier score: n/a
- Average calibrated log loss: n/a
- Average calibrated accuracy: n/a
- Average calibrated ECE: n/a
- Average calibrated Brier improvement: n/a
- Average calibrated log loss improvement: n/a
- Average calibrated accuracy delta: n/a
- Average calibrated ECE improvement: n/a
- Average replay net PnL: 0.6000
- Average replay return %: 0.6000
- Average replay fill rate: 1.0000
- Average replay complete-fill rate: 1.0000
- Average replay partial-fill rate: 0.0000
- Average replay fill ratio: 1.0000
- Average replay wait steps: 0.0000
- Average replay slippage (bps): 0.0000
- Replay stale rows: 0
- Average signal edge (bps): 966.6667
- Average execution drag (bps): 0.0000
- Average model residual (bps): -166.6667
- Average closing edge (bps): 800.0000
- Edge ledger rows: 4
- Execution ledger rows: 3

## Case table

| Case | Fair-value Brier | Fair-value Log Loss | Replay Net PnL | Replay Return % |
|---|---:|---:|---:|---:|
| sports-benchmark-best-line | 0.243711 | 0.680568 | n/a | n/a |
| sports-benchmark-round-trip | n/a | n/a | 0.7000 | 0.7000 |
| sports-benchmark-tiny | 0.180625 | 0.553385 | 0.5000 | 0.5000 |

## Fair-value baseline deltas

| Baseline | Case Count | Avg Brier Delta (primary - baseline) | Avg Log Loss Delta (primary - baseline) |
|---|---:|---:|---:|
| bookmaker_multiplicative_best_line | 2 | 0.000000 | 0.000000 |
| bookmaker_multiplicative_independent | 2 | -0.008435 | -0.016872 |
| bookmaker_power_independent | 2 | -0.007739 | -0.015429 |
| market_midpoint | 1 | 0.032111 | 0.064382 |

## Fair-value paired comparison stats

Percentile bootstrap confidence intervals are computed on the mean paired loss differential. DM-style z-scores and p-values use a two-sided normal approximation on primary-minus-comparison row losses, so negative values favor the primary fair value.

| Comparison | Metric | Cases | Rows | Mean Diff | Bootstrap CI | DM-style z | p-value |
|---|---|---:|---:|---:|---|---:|---:|
| bookmaker_multiplicative_best_line | brier_error | 2 | 4 | 0.000000 | 95% CI [0.000000, 0.000000] | 0.000000 | 1.000000 |
| bookmaker_multiplicative_best_line | log_loss | 2 | 4 | 0.000000 | 95% CI [0.000000, 0.000000] | 0.000000 | 1.000000 |
| bookmaker_multiplicative_independent | brier_error | 2 | 4 | -0.008435 | 95% CI [-0.016870, 0.000000] | -1.732051 | 0.083265 |
| bookmaker_multiplicative_independent | log_loss | 2 | 4 | -0.016872 | 95% CI [-0.033744, 0.000000] | -1.732051 | 0.083265 |
| bookmaker_power_independent | brier_error | 2 | 4 | -0.007739 | 95% CI [-0.017639, 0.002161] | -1.353972 | 0.175745 |
| bookmaker_power_independent | log_loss | 2 | 4 | -0.015429 | 95% CI [-0.035282, 0.004425] | -1.346011 | 0.178299 |
| market_midpoint | brier_error | 1 | 2 | 0.032111 | 95% CI [0.032111, 0.032111] | 1156918814370065.000000 | 0.000000 |
| market_midpoint | log_loss | 1 | 2 | 0.064382 | 95% CI [0.064382, 0.064382] | n/a | n/a |

## Replay baseline deltas

| Baseline | Case Count | Avg Net PnL Delta (primary - baseline) | Avg Return % Delta (primary - baseline) |
|---|---:|---:|---:|
| noop_strategy | 2 | 0.6000 | 0.6000 |

## Replay execution realism

| Case | Fill Rate | Complete Fill Rate | Partial Fill Rate | Avg Fill Ratio | Avg Wait Steps | Avg Slippage (bps) | Stale Rows |
|---|---:|---:|---:|---:|---:|---:|---:|
| sports-benchmark-round-trip | 1.0000 | 1.0000 | 0.0000 | 1.0000 | 0.0000 | 0.0000 | 0 |
| sports-benchmark-tiny | 1.0000 | 1.0000 | 0.0000 | 1.0000 | 0.0000 | 0.0000 | 0 |

## Replay attribution summary

| Case | Trades | Avg Signal Edge (bps) | Avg Execution Drag (bps) | Avg Model Residual (bps) | Avg Closing Edge (bps) | Total PnL |
|---|---:|---:|---:|---:|---:|---:|
| sports-benchmark-round-trip | 2 | 950.0000 | 0.0000 | -250.0000 | 700.0000 | 0.7000 |
| sports-benchmark-tiny | 1 | 1000.0000 | 0.0000 | 0.0000 | 1000.0000 | 0.5000 |
