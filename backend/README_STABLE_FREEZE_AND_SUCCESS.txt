CHEMLENS stable freeze + success status

Files:
- freeze_current_canonical_state.py
- show_campaign_success_status.py
- run_freeze_current_canonical_state.bat

1) Confirm how many campaign families are resolved
   conda activate chemlens
   cd /d C:\chemlens\backend
   python show_campaign_success_status.py

2) Freeze current canonical as the new stable snapshot
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_freeze_current_canonical_state.bat

Outputs:
- backups\stable_freeze\YYYYMMDD_HHMMSS\labint.stable_YYYYMMDD_HHMMSS.db
- backups\stable_freeze\YYYYMMDD_HHMMSS\labint_v5_stage.stable_YYYYMMDD_HHMMSS.db
- reports\stable_freeze\YYYYMMDD_HHMMSS\stable_freeze_summary.json
- reports\stable_freeze\YYYYMMDD_HHMMSS\stable_freeze_summary.md

The campaign status script checks the 13-family rejected campaign that was fixed over time:
Diels-Alder, Fries, Houben-Hoesch, Finkelstein, Hunsdiecker,
Claisen, HWE, Krapcho, Michael, Regitz, Enyne, HLF, Mitsunobu.
