CHEMLENS Gemini Family Automation v2

What changed in v2
- Added include-families filter (comma-separated exact family names, normalized internally)
- Added exclude-families filter
- Added rollback-on-regression=yes/no
- Added safer invalid-SMILES handling: malformed SMILES are skipped instead of crashing the whole candidate insert
- Recommended continuation path now uses benchmark-every=1 and phase3 safe families first

Why this patch exists
- The first chaos run proved the engine works: coverage increased from 41 to 46 and queryable from 443 to 462.
- The run stopped because a benchmark regression appeared after 5 inserts.
- The likely cause is that chaos-mode allowed risky generic families (especially Aldol / Barbier class candidates) to enter too early.
- This patch keeps automation moving by rolling back only the offending candidate and continuing to the next candidate.

Recommended next run
1) Open Anaconda Prompt
2) conda activate chemlens
3) cd /d C:\chemlens\backend
4) Run:
   run_gemini_family_automation.bat

Recommended direct command
python gemini_family_automation.py --reset-stage --db app\labint.db --stage-db app\labint_gemini_autorun_safe.db --family-target 60 --candidate-limit 12 --max-rounds 12 --allow-generic no --benchmark yes --benchmark-every 1 --stop-on-regression yes --rollback-on-regression yes --include-families "Birch Reduction,Baylis-Hillman Reaction,Carroll Rearrangement" --report-dir reports\gemini_family_automation_safe

If you want to add more safe families later
- Extend include-families instead of turning chaos-mode back on immediately.
- Suggested next expansion after the first safe trio stabilizes:
  Corey-Kim Oxidation
  Corey-Fuchs Alkyne Synthesis
  Corey-Nicolaou Macrolactonization
  Combes Quinoline Synthesis

Important rules
- Canonical DB remains: C:\chemlens\backend\app\labint.db
- Do not point automation at old bridge/work DBs
- Keep benchmark enabled
- Prefer include-families over chaos-mode until rollback logs show stable behavior


[2026-04-18 v3 continuous mode]
- Adds --run-through yes for uninterrupted bounded runs.
- Failures/regressions are quarantined and skipped later in the same run.
- Empty rounds widen candidate_limit and retry instead of stopping immediately.
- Writes quarantined_families.json in each run report directory.
