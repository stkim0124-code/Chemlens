CONTINUOUS EXPAND V5 CAMPAIGN

Files:
- continuous_gemini_expand_v5.py
- run_expand_v5_continuous.bat

Mechanism preserved from the successful rejected-family campaign:
1) Never mutate canonical during screening.
2) Build one Gemini candidate at a time.
3) Screen candidate on a TEMP DB with benchmark.
4) Freeze PASS candidate to JSON.
5) Apply the exact frozen candidate to canonical.
6) Re-run benchmark after each apply.
7) If post-apply benchmark regresses, restore backup.

Default runner behavior:
- family_target=305
- families_per_round=12
- max_attempts=3 per family per round
- effectively continuous (very large round / empty-round limits)
- benchmark file prefers benchmark\named_reaction_benchmark_v4.json if present, otherwise falls back to benchmark\named_reaction_benchmark_small.json

Usage:
conda activate chemlens
cd /d C:\chemlens\backend
run_expand_v5_continuous.bat
