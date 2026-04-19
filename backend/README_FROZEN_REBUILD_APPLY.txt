Use this patch after a successful DRY-RUN of gemini_rebuild_rejected8 where some families PASS,
but APPLY lost one or more PASS families because Gemini was called again.

This script reuses the exact accepted candidates recorded in the DRY-RUN summary JSON.
By default it:
- loads the latest DRY-RUN summary under reports\gemini_rebuild_rejected8\
- loads the latest APPLY summary under reports\gemini_rebuild_rejected8\ and excludes already applied families
- applies only the remaining accepted families
- benchmarks after each applied family

Typical use:
  conda activate chemlens
  cd /d C:\chemlens\backend
  run_apply_frozen_gemini_rebuild_candidates.bat
