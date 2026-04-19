CHEMLENS V5 SELECTIVE MERGE FIXED

What changed
- Fixed benchmark JSON parsing: reads summary.top1_accuracy / summary.top3_accuracy
- Added sanity guard: aborts if baseline benchmark parses as 0.0 / 0.0
- Use this only after restoring canonical from the backup created before selective merge

Recommended steps
1. Restore canonical backup from the failed selective merge run
   copy /Y app\labint.backup_before_v5_selective_merge_20260418_155258.db app\labint.db
2. Confirm benchmark is back to 1.0 / 1.0
3. Run dry-run
   python merge_v5_stage_selective.py --dry-run
4. Run apply
   python merge_v5_stage_selective.py --apply
5. Re-run benchmark and VERIFY_CURRENT_BACKEND_STATE.py
