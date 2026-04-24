# Phase 4 Gate B — Phase 3: Missed Benchmark Items Distribution

**Created:** 2026-04-24
**Purpose:** User asked ("위 세가지 이외에 제가 혹여나 빠트린 benchmark 처리가 되지 않은 것들이 있다면 어떤 것들이 있는지 정리해주시고 그 것들은 어떻게 처리할지 고민 후에 계획을 보고해주세요"). This document enumerates 10 items surfaced during Gate A/B audit that are not blocking but improve benchmark hygiene, then distributes them into "in-scope for Gate B" (5) vs "post-Gate B" (5).

---

## Master list (10 items)

| # | Item | Observed during | One-line impact |
|---|---|---|---|
| 1 | Unique-edge metric (how many cases move uniquely between phases) | 3g→3f diff ambiguity | Separates "real gain" from "two bugs cancelling" |
| 2 | 4 `no_confident_hit_cases` never analysed since 3c-b | 3g-5 residual inspection | Unknown failure mode persists in r@5 denominator |
| 3 | broad/coverage case-level diff (only admission gets it today) | Gate A flip audit | Gate B may silently regress broad/coverage even if admission passes |
| 4 | Cache invalidation pre-check before benchmark | Phase 3c gotcha | `_family_coarse_profile` cache reuse → false-same results |
| 5 | `source_kind` split in diff (queryable=1 vs =0 rows) | Phase 4 Gate A flip | Can't tell whether a delta originates from safe rows or OCSR park rows |
| 6 | Regression trace matrix (which guards caused which case to move which direction) | Phase 3e-b-1 Cope regression | Point fixes become archaeology if not traced at apply-time |
| 7 | Task system has stale `#39 pending` (Cope/Retro-Claisen regression) | TaskList dump | Open pending blocks TaskList hygiene |
| 8 | No `benchmark_timeline.csv` rolling up all phases' metrics | Memory consolidation | User reads 10 different `apply_summary.json` files to see trend |
| 9 | UTF-8-sig BOM in `family_admission_confusion.csv` header | Python readers break | Silent KeyError on `expected_canonical` col |
| 10 | `reports/` folder has accumulated ~60 phase-subdirs, no index | File-tree survey | Hard to find latest vs archived results |

---

## Distribution decisions

### In-scope for Gate B (5 items) — block guard shipping if not done

| # | Item | Why in-scope | Who owns |
|---|---|---|---|
| 1 | Unique-edge metric | Gate B is the first phase with merge + targeted guards → prevent double-counting from day 1 | Gate B eval harness |
| 3 | broad/coverage case-level diff | Gate B will add SMARTS features that affect coverage; blind spot is too large to accept | Gate B eval harness |
| 5 | `source_kind` split in diff | Phase 4 Gate A found step4_ocsr rows have adversarial pull; must track their delta separately going forward | Gate B eval harness |
| 6 | Regression trace matrix | 13 guards landing at once → without trace, rollback granularity is lost | Gate B apply orchestrator |
| 9 | UTF-8-sig BOM handling | Already biting Python readers; cheap `encoding="utf-8-sig"` fix | Gate B eval harness |

**Gate B entry criterion:** Items 1/3/5/6/9 implemented in the eval harness BEFORE the 13-guard diff is computed.

### Post-Gate B (5 items) — not blocking, registered for follow-up

| # | Item | Why deferred | Scheduled for |
|---|---|---|---|
| 2 | Analyse 4 `no_confident_hit_cases` | These have been "silent 4" since 3c-b; another phase won't hurt. Needs standalone attention. | Gate C or Phase 5 |
| 4 | Cache invalidation pre-check | Already mitigated via `conda run` fresh-process pattern; formalization is nice-to-have | Gate D / housekeeping |
| 7 | Close/update task #39 | 5-minute triage task | End of Gate B (post-#117 close) |
| 8 | `benchmark_timeline.csv` | Nice artifact, not required for Gate B's decision | Post-Gate B, before Gate C |
| 10 | `reports/` folder restructure | Pure housekeeping; no impact on runs | After Gate C ships |

---

## Implementation sketch for the 5 in-scope items

### Item 1 — Unique-edge metric
Add to eval harness post-process:
```
unique_edges_gained  = |cases that passed now && failed in baseline|
unique_edges_lost    = |cases that failed now && passed in baseline|
net_edge_delta       = gained − lost  (replaces raw adm_top1 delta as the headline stat)
```

### Item 3 — broad/coverage case-level diff
Extend `scripts/diff_benchmark_results.py` (or equivalent) to emit:
```
reports/phase4/gateB/apply_<TS>/broad/case_level_diff.csv
reports/phase4/gateB/apply_<TS>/coverage/case_level_diff.csv
```
Same schema as admission's existing diff.

### Item 5 — source_kind split
Join case_ids to `extract_molecules.source_kind` and emit:
```
edges_gained_by_source_kind = {"queryable_1": N1, "queryable_0": N2}
edges_lost_by_source_kind   = {"queryable_1": N3, "queryable_0": N4}
```
This surfaces Gate A–style adversarial effects immediately.

### Item 6 — Regression trace matrix
During apply_orchestrator, for each of the 13 guards:
1. Take a snapshot diff BEFORE that guard landed
2. Take a snapshot diff AFTER that guard landed
3. Attribute each case movement to the delta of the first guard that touched it

Emit `reports/phase4/gateB/apply_<TS>/regression_trace_matrix.csv`:
```
case_id, direction(gained|lost|no_change), attributed_to_guard, confidence
```

### Item 9 — UTF-8-sig BOM
Single-line fix in eval readers:
```python
# Before: pd.read_csv(path)
# After:  pd.read_csv(path, encoding="utf-8-sig")
```
Apply to all `reports/**/family_admission_confusion.csv` readers.

---

## Task registrations

| Task ID | For | Status |
|---|---|---|
| #121 (new) | Items 1/3/5/6/9 — Gate B eval harness upgrade | To be created |
| #122 (new) | Item 7 — Triage/close task #39 | To be created |
| #123 (new) | Item 2 — Analyse 4 `no_confident_hit_cases` | To be created |
| Deferred | Items 4/8/10 — housekeeping cluster | Logged here; not a task yet |

**Distribution rule:** Only tasks #121 (5 in-scope items) is a Gate B blocker. #122/#123 run parallel or after.
