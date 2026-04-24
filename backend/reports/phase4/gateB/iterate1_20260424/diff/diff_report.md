# Gate B eval harness diff report

- **before:** `/sessions/amazing-adoring-ramanujan/mnt/chemlens/backend/reports/phase3g/apply_3g_5_20260422_000000`
- **after:** `/sessions/amazing-adoring-ramanujan/mnt/chemlens/backend/reports/phase4/gateB/iterate1_20260424`
- **defects excluded:** 3 (adm_stille_carbonylative_cross-coupling_1303, adm_stille_cross-coupling_(migita-kosugi-stille_coupling)_1306, adm_wittig_reaction_466)

## Per-layer unique-edge metric

| layer | total | gained | lost | net | stayed_pass | stayed_fail |
|---|---:|---:|---:|---:|---:|---:|
| admission | 256 | 5 | 0 | +5 | 197 | 54 |
| broad | 259 | 5 | 0 | +5 | 197 | 57 |
| coverage | 518 | 2 | 1 | +1 | 457 | 58 |

## source_kind split (gained | lost)

| layer | queryable_1 (+/−) | queryable_0 (+/−) | unknown (+/−) |
|---|:---:|:---:|:---:|
| admission | 0 / 0 | 0 / 0 | 0 / 0 |
| broad | 0 / 0 | 0 / 0 | 5 / 0 |
| coverage | 0 / 0 | 0 / 0 | 0 / 0 |

## Gate verdict

- **pass?** True — admission net=+5, coverage net=+1; heuristic rule: admission must gain and coverage must not regress.

