Apply order:
1) Run run_claisen_specific_rebuild_loop.bat first.
2) If Claisen is applied successfully and benchmark stays clean, run run_krapcho_specific_rebuild_loop.bat.

This patch uses a frozen-pass workflow:
- Gemini generates one candidate per attempt.
- Candidate is screened in a temp DB with the small benchmark.
- Only a PASS candidate is frozen and immediately reused for real apply.
- Real apply is benchmark-checked again.

Outputs:
reports\gemini_rebuild_single_family_loop\<timestamp>_<family>\single_family_loop_summary.json
