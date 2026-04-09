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
- Average replay net PnL: 0.6000
- Average replay return %: 0.6000

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

## Replay baseline deltas

| Baseline | Case Count | Avg Net PnL Delta (primary - baseline) | Avg Return % Delta (primary - baseline) |
|---|---:|---:|---:|
| noop_strategy | 2 | 0.6000 | 0.6000 |
