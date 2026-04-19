SWERN OXIDATION single-family rebuild APPLY patch

Files:
- apply_single_family_rebuild.py
- run_apply_swern_round1_dryrun.bat
- run_apply_swern_round1_apply.bat

Usage:
1) Dry-run guard recheck
   conda activate chemlens
   cd /d C:\chemlensackend
   run_apply_swern_round1_dryrun.bat

2) If dry-run PASS, actual apply
   conda activate chemlens
   cd /d C:\chemlensackend
   run_apply_swern_round1_apply.bat

This script automatically finds the latest reports\gemini_single_family_rebuild\...\gemini_single_family_rebuild_summary.json
and applies the accepted candidate only if benchmark guard stays clean.
