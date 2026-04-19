Apply this ZIP into C:\chemlens\backend and run:

conda activate chemlens
cd /d C:\chemlens\backend
run_gemini_rebuild_pinner_round1.bat

This is DRY-RUN only.
It will generate up to 3 single-candidate Gemini rebuild attempts for Pinner Reaction,
insert each candidate into a temp DB, and run the named-reaction small benchmark guard.

Result summary:
reports\gemini_single_family_rebuild\<timestamp>\gemini_single_family_rebuild_summary.json
