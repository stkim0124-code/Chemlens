# ChemLens v4.5 patch

This patch is for the **current frontier** where plan-only shows mostly:
- `lane=gemini`
- `seed=0`
- `extracts=0`

That means the run is in **discovery mode**, not deterministic promotion mode.

## What changed

1. **Gemini response normalization**
   - Handles top-level dict, string-wrapped JSON, fenced JSON, and list payloads.
   - Prevents crashes like:
     - `'list' object has no attribute 'get'`
   - Invalid schemas are now recorded as clean failures instead of throwing exceptions.

2. **Discovery frontier detection**
   - If top candidates are all `gemini / seed=0 / extracts=0`, the runner enters discovery mode.
   - Discovery mode automatically:
     - limits attempts to the first 3 candidates per round
     - uses `batch_size=1`
     - stops after the first zero-progress round

3. **Pseudo-family filtering**
   - Filters out non-reaction entries such as rules/guidelines/principles/classification-style pages.
   - Reduces garbage candidates like Baldwin rules / generic guidance pages.

4. **Page-grounded Gemini prompt**
   - Adds page title, section name, summary, notes, and image hint to the prompt.
   - Makes zero-evidence generation more grounded than family-name-only prompting.

5. **Safer batch runner**
   - Updated `run_v4_automation.bat` for short discovery-safe runs.
   - No more long 20-round zero-insert loops.

## Expected behavior

If discovery still fails, the run should now stop quickly with a reason like:
- `zero_insert_round_discovery`

That is expected and preferable to wasting rounds with no inserts.
