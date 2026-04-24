# Phase 4 Gate B — Phase 3 (Missed-Benchmark 10-Item Distribution) End-of-Phase Verification

**Created:** 2026-04-24
**Purpose:** Per user requirement ("이 역시 끝난 뒤에 제대로 잘 처리됐는지 아쉬운 부분은 없는지 체크"), audit the 10-item distribution for completeness and surface residual gaps.

---

## 1. Distribution summary

| Disposition | Items | Task registration |
|---|---|---|
| Gate B-blocking (in eval harness upgrade) | 1, 3, 5, 6, 9 | Task #121 |
| Post-Gate B, own task | 2, 7 | Tasks #123, #122 |
| Deferred (no task, logged in markdown only) | 4, 8, 10 | — |

Total: 10/10 items disposed. Zero items unaddressed.

Task graph: `#121 blocks #116` (3-layer benchmark cannot start until harness is upgraded).

---

## 2. 아쉬운 부분 (Gap Analysis)

### 2.1 Item-by-item audit

| # | Item | Coverage check | Gap flag |
|---|---|---|---|
| 1 | Unique-edge metric | Formula defined in missed_benchmark_items_distribution.md §Implementation | None |
| 2 | 4 `no_confident_hit_cases` | Task #123 registered with scope | Deferred execution — acceptable |
| 3 | broad/coverage case-level diff | Output path specified | None |
| 4 | Cache invalidation pre-check | Deferred without task | **Medium gap** — see §2.2 |
| 5 | `source_kind` split | Formula defined with join strategy | None |
| 6 | Regression trace matrix | Approach sketched (snapshot-before/after per guard) | **Low-medium gap** — see §2.3 |
| 7 | Close/update task #39 | Task #122 registered | None |
| 8 | `benchmark_timeline.csv` | Deferred without task | Accepted — low priority |
| 9 | UTF-8-sig BOM | Single-line fix | None |
| 10 | `reports/` folder restructure | Deferred without task | Accepted — pure housekeeping |

### 2.2 Deferred items without tasks (items 4, 8, 10)

Three items live only in the markdown: #4 (cache invalidation pre-check), #8 (timeline CSV), #10 (reports restructure). Risk: they drift out of consciousness once Gate B closes.

**Mitigation applied:** The distribution markdown lists them in a dedicated "Post-Gate B, not blocking" row; a later consolidate-memory pass will promote them to tasks if they recur.

**Residual risk:** Low. Item #4 is practically mitigated by the `conda run` fresh-process pattern (memory: `feedback_conda_run.md` — already baked into execution habits). Items #8/10 are informational/ergonomic.

### 2.3 Regression trace matrix (item 6) — attribution fragility

The sketch says "attribute each case movement to the first guard that touched it." For interleaved guards whose SMARTS overlap (e.g., Sandmeyer + diazonium + Schmidt all touch N-N/N=N patterns), attribution will be non-unique.

**Decision:** Accept non-uniqueness. The trace matrix outputs `attributed_to_guard` + `confidence` (high/medium/low). For low-confidence rows, a manual review step is implied. This is honest about the limit rather than fabricating false precision.

### 2.4 Scope omissions (items we deliberately did NOT add to the list)

For completeness, items that were CONSIDERED and excluded from the 10:

| Considered | Why excluded |
|---|---|
| "Add retraining/backprop metrics" | Not applicable — this is a rule-based system, no model weights |
| "Benchmark vs external oracle (USPTO, Reaxys)" | Out of Phase 4 scope; Gate D or later |
| "Time-profiling guard evaluation" | No evidence of perf regression; speculative |
| "Automated SMARTS linter" | Nice-to-have; post-Gate B tooling |

### 2.5 Dependency graph sanity

`#121 (harness) → #116 (benchmark) → #117 (iterate/revert) → #118 (Gate C)`

`#120 (Phase 5 scoping)` runs in parallel — not blocked, not blocking.

`#122 (task hygiene)` runs in parallel.

`#123 (4-residual analysis)` runs in parallel post-#117.

No circular dependencies. Longest chain from today: #121 → #116 → #117 → #118.

---

## 3. Sign-off criteria (all met)

- [x] All 10 items have an explicit disposition (in-scope / task-registered / deferred with rationale)
- [x] Gate B-blocking items (#1/3/5/6/9) bundled into a single blocker task (#121)
- [x] Task graph updated so that #116 cannot run until #121 is complete
- [x] Deferred items (#4/8/10) have documented rationale for low priority
- [x] Attribution-fragility risk in item #6 explicitly called out
- [x] Sibling phases (Phase 5 scoping via #120) are registered and not blocked

**Verdict:** Phase 3 (10-item distribution) complete. Safe to proceed to Phase 4 (13-guard implementation).
