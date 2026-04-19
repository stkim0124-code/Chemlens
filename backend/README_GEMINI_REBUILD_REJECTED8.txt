Gemini rebuild rejected 8 seeds

Purpose:
- Move past direct stage-seed injection failures.
- Use Gemini 2.5 Pro aggressively but safely on the remaining 8 rejected families.
- For each family, generate up to 3 replacement candidates, benchmark-screen each one in a temp DB,
  and only accept/apply candidates that preserve top1=1.0, top3=1.0, violations=0.

Families targeted:
- Claisen Condensation / Claisen Reaction
- Horner-Wadsworth-Emmons Olefination
- Krapcho Dealkoxycarbonylation
- Michael Addition Reaction
- Regitz Diazo Transfer
- Enyne Metathesis
- Hofmann-Loffler-Freytag Reaction
- Mitsunobu Reaction

What this does:
- Reads GEMINI_API_KEY from backend\.env or environment.
- Forces Gemini usage family-by-family.
- Uses cluster-specific anti-copy prompts:
  - Buchner cluster: avoid diazo_arene_combo / ring_expansion topologies
  - Barton cluster: avoid decarboxylation / deoxygenation topologies
- Validates returned SMILES with RDKit.
- Inserts candidate into a temp DB and runs small benchmark.
- Only candidates that fully pass guard are marked accepted.
- In APPLY mode, only accepted families are inserted into canonical.

Usage:
1) Unzip into C:\chemlens\backend and overwrite:
   - gemini_rebuild_rejected8_seeds.py
   - run_gemini_rebuild_rejected8.bat
2) Run:
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_gemini_rebuild_rejected8.bat

Key output:
- reports\gemini_rebuild_rejected8\<timestamp>\gemini_rebuild_rejected8_summary.json


V3 fix: request path now mirrors the proven salvage runner payloads and records the full HTTP response text inside errors.
