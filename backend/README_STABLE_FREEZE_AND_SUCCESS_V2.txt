CHEMLENS stable freeze + acquisition status v2

Files:
- freeze_current_canonical_state.py
- show_campaign_success_status.py
- show_backend_acquisition_status.py
- run_freeze_current_canonical_state.bat
- run_show_backend_acquisition_status.bat

1) Broad totals (total family registry + current acquired data volume)
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_show_backend_acquisition_status.bat

2) Campaign-only resolved family count
   python show_campaign_success_status.py

3) Freeze current canonical as stable snapshot
   run_freeze_current_canonical_state.bat
