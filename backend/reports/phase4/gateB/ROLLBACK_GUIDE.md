# Phase 4 Gate B — Rollback Guide (2026-04-24)

## Instant rollback (if benchmark regresses catastrophically)

```cmd
cd C:\chemlens\backend\app
copy /Y evidence_search.py.bak_phase4_gateB_pre_20260424_090035 evidence_search.py
```

DB is untouched by Gate B (evidence_search edits only). If labint.db somehow
got modified, revert with:
```
copy /Y C:\chemlens\backend\merge_backups\labint_phase4gateB_pre_20260424_090035.db C:\chemlens\backend\app\labint.db
```

## Safety layers in place

1. **primary backup**: `app/evidence_search.py.bak_phase4_gateB_pre_20260424_090035`
2. **redundant copy**: `app/evidence_search.py.bak_phase4_gateB_safety_20260424_090035`
3. **db snapshot**: `merge_backups/labint_phase4gateB_pre_20260424_090035.db`
4. **staged patch strategy**: all Gate B edits share `# --- Phase 4 Gate B start/end ---`
   markers so surgical revert is possible (cut the block out, re-test).
5. **guard-by-guard retry protocol** (see fix-don't-bypass memory):
   - If benchmark regresses, DO NOT revert whole file blindly.
   - Identify offending guard via case-level diff (`diff_confusion.py`).
   - Patch that one guard, re-test; keep all other Gate B guards.

## Verification after rollback

Expected adm_top1 post-rollback = 0.7606, cov_r5 = 0.8842 (3g-5 baseline).
Gate A already confirmed DB is at bit-identical 3g-5 state.

