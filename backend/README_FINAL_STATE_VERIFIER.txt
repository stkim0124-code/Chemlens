Files:
- verify_final_state.py
- run_verify_final_state.bat

Use:
  conda activate chemlens
  cd /d C:\chemlens\backend
  run_verify_final_state.bat

What it checks:
1) Current canonical benchmark (top1/top3/violations)
2) Presence of the 12 short-form resolved families in canonical
3) Queryable molecule counts for those families
4) Alias/ontology cleanup for the 3 long-form names
5) Writes reports under reports\final_state_verification\<timestamp>\
