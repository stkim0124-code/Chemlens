# Gate B eval harness diff report

- **before:** `C:\chemlens\backend\reports\phase3g\apply_3g_5_20260422_000000`
- **after:** `C:\chemlens\backend\reports\phase4\gateB\apply_20260424_bench`

## Per-layer unique-edge metric

| layer | total | gained | lost | net | stayed_pass | stayed_fail |
|---|---:|---:|---:|---:|---:|---:|
| admission | 259 | 5 | 2 | +3 | 195 | 57 |
| broad | 259 | 5 | 2 | +3 | 195 | 57 |
| coverage | 518 | 3 | 5 | -2 | 453 | 57 |

## source_kind split (gained | lost)

| layer | queryable_1 (+/−) | queryable_0 (+/−) | unknown (+/−) |
|---|:---:|:---:|:---:|
| admission | 0 / 0 | 0 / 0 | 0 / 0 |
| broad | 0 / 0 | 0 / 0 | 5 / 2 |
| coverage | 0 / 0 | 0 / 0 | 0 / 0 |

## Gate verdict

- **pass?** False — admission net=+3, coverage net=-2; heuristic rule: admission must gain and coverage must not regress.

