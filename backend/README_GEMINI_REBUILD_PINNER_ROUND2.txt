Files in this patch:
- gemini_rebuild_single_family.py
- family_prompts\pinner_reaction_round2.json
- run_gemini_rebuild_pinner_round2.bat

How to use:
1. Extract this ZIP into C:\chemlens\backend
2. Run:
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_gemini_rebuild_pinner_round2.bat

Purpose:
- Retry Pinner Reaction with a stricter anti-hijack prompt
- Prefer substituted aryl nitrile -> imidate ester examples
- Avoid generic alkyl nitrile seeds and reagent SMILES
