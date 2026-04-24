# Gate B eval harness diff report

- **before:** `C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424`
- **after:** `C:\chemlens\backend\reports\phase4\gateB\iter2_20260424_203319`
- **defects excluded:** 3 (adm_stille_carbonylative_cross-coupling_1303, adm_stille_cross-coupling_(migita-kosugi-stille_coupling)_1306, adm_wittig_reaction_466)

## Per-layer unique-edge metric

| layer | total | gained | lost | net | stayed_pass | stayed_fail |
|---|---:|---:|---:|---:|---:|---:|
| admission | 256 | 1 | 1 | +0 | 201 | 53 |
| broad | 259 | 1 | 1 | +0 | 201 | 56 |
| coverage | 518 | 0 | 0 | +0 | 459 | 59 |

## source_kind split (gained | lost)

| layer | queryable_1 (+/−) | queryable_0 (+/−) | unknown (+/−) |
|---|:---:|:---:|:---:|
| admission | 0 / 0 | 0 / 0 | 0 / 0 |
| broad | 0 / 0 | 0 / 0 | 1 / 1 |
| coverage | 0 / 0 | 0 / 0 | 0 / 0 |

## Gate verdict

- **pass?** False — admission net=+0, coverage net=+0; heuristic rule: admission must gain and coverage must not regress.

