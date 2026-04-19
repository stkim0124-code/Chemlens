CHEMLENS rejected8 evidence_search patch

What changed
- Added explicit coarse-profile penalties for the active rejected 8 families.
- Buchner-cluster families now get strong penalties on diazo_arene_combo / ring_expansion signals.
- Barton-cluster families now get strong penalties on decarboxylation / deoxygenation signals.

Target families
- Buchner cluster:
  * Claisen Condensation / Claisen Reaction
  * Horner-Wadsworth-Emmons Olefination
  * Krapcho Dealkoxycarbonylation
  * Michael Addition Reaction
  * Regitz Diazo Transfer
- Barton cluster:
  * Enyne Metathesis
  * Hofmann-Loffler-Freytag Reaction
  * Mitsunobu Reaction

Apply
1. Extract this ZIP into C:\chemlens\backend
2. Overwrite app\evidence_search.py
3. Re-run the inject script:
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_inject_rejected_8_families.bat

Expected effect
- inject v3 should now test the same 8 stage seeds against a scorer with stronger family-specific penalties.
- Some or all of the 8 may move from REJECTED to ACCEPTED if the benchmark hijack was primarily coarse-profile driven.
