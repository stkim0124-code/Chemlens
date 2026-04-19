Apply:
1. Extract this ZIP into C:\chemlens\backend
2. Overwrite the existing files if prompted
3. Run from Anaconda Prompt:

   conda activate chemlens
   cd /d C:\chemlens\backend
   run_gemini_salvage_next2_round2.bat

This patch fixes:
- one-candidate-per-request generation to avoid MAX_TOKENS truncation
- much shorter prompts and shorter stage-example context
- fallback retry with a relaxed prompt if the richer prompt overflows
- robust JSON extraction without crashing the whole run on a single family
