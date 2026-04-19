
CHEMLENS v5 rejected-family diagnosis + Gemini salvage patch

Files
- diagnose_v5_rejected_families.py
- gemini_salvage_rejected_families.py
- run_diagnose_v5_rejected_families.bat
- run_gemini_salvage_rejected_families.bat

What this patch is for
1) Diagnose WHY the remaining rejected families fail.
2) Identify exactly which benchmark case(s) get displaced.
3) Generate Gemini salvage proposals for the stubborn families.

Important
- The diagnosis script does NOT change canonical.
- It makes a temp copy of canonical for each family/variant, inserts only that subset, runs the benchmark, and records diffs.
- The Gemini salvage script in this version is proposal-only by default. It prints one discriminative textbook example per stubborn family. It does not auto-apply to canonical yet.

Recommended use
1. Copy files into C:\chemlens\backend
2. Activate environment
   conda activate chemlens
   cd /d C:\chemlens\backend
3. Run diagnosis
   python diagnose_v5_rejected_families.py
4. Inspect:
   reports\v5_rejected_diagnose\...\rejected_diagnosis_summary.json
   and per-family JSON files in the same folder.
5. Then run Gemini proposal generation for the still-rejected set:
   set GEMINI_API_KEY=your_key_here
   python gemini_salvage_rejected_families.py --diag-summary <path-to-rejected_diagnosis_summary.json>

What to look for
- changed_cases
- first_changed_case
- baseline_top1_family -> current_top1_family
- whether the failure is consistently 0.9630 or 0.9259
- whether one benchmark case is repeatedly displaced by the same new family

Interpretation guide
- If all variants fail and the same case keeps flipping to the same wrong family:
  this is a scorer/family-prior conflict, not just an evidence-size problem.
- If minimal_pair passes but larger variants fail:
  the family can still be salvaged by a narrower canonical example.
- If even minimal_pair fails:
  use the Gemini proposal to regenerate a maximally discriminative textbook example.
