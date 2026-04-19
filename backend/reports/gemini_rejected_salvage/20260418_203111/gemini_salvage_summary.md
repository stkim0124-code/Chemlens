# Gemini salvage summary

- model: `gemini-2.5-pro`
- diag_summary: `C:\chemlens\backend\reports\v5_rejected_diagnose\20260418_201257\rejected_diagnosis_summary.json`
- total_families: 2
- generated_at: `2026-04-18T20:33:29`

## Enyne Metathesis

- changed_cases: 2
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Enyne Metathesis`
- cluster: `barton`
- error: Failed to parse Gemini response after retries: Gemini response did not contain text parts (finish_reason=MAX_TOKENS); raw={"candidates": [{"content": {"role": "model"}, "finishReason": "MAX_TOKENS", "index": 0}], "usageMetadata": {"promptTokenCount": 1513, "totalTokenCount": 5606, "promptTokensDetails": [{"modality": "TEXT", "tokenCount": 1513}], "thoughtsTokenCount": 4093}, "modelVersion": "gemini-2.5-pro", "responseId": "xWvjaeWWBYDj0-kPiYqA-A0"}

## Hofmann-Loffler-Freytag Reaction

- changed_cases: 2
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Hofmann-Loffler-Freytag Reaction`
- cluster: `barton`
- error: Failed to parse Gemini response after retries: Unterminated string starting at: line 30 column 35 (char 1534); raw={"candidates": [{"content": {"parts": [{"text": "```json\n{\n  \"family\": \"Hofmann-Loffler-Freytag Reaction\",\n  \"candidates\": [\n    {\n      \"candidate_id\": \"C1\",\n      \"substrate_smiles\": [\n        \"CCCCN(C)Cl\"\n      ],\n      \"product_smiles\": [\n        \"CN1CCCC1\"\n      ],\n      \"optional_reagent_smiles\": [\n        \"O=S(=O)(O)O\"\n      ],\n      \"rationale\": \"This is a classic textbook example of the Hofmann-Loffler-Freytag reaction. The N-chloro-N-methylbutanamine, upon treatment with acid and light/heat, undergoes a 1,5-hydrogen atom transfer from the delta-carbon to the nitrogen radical, followed by cyclization to form the stable five-membered pyrrolidine ring.\",\n      \"collision_avoidance_note\": \"This example is a canonical HLF reaction. The substrate is a simple N-haloamine, and the product is a cyclic amine formed by intramolecular C-H amination. It lacks any features associated with Barton reactions, such as thiohydroxamate esters or decarboxylation, ensuring high specificity.\"\n    },\n    {\n      \"candidate_id\": \"C2\",\n      \"substrate_smiles\": [\n        \"CCCCCN(C)Cl\"\n      ],\n      \"product_smiles\": [\n        \"CN1CCCCC1\"\n      ],\n      \"optional_reagent_smiles\": [\n        \"O=S(=O)(O)O\"\n      ],\n      \"rationale\": \"This example demonstrates the formation of a six-membered piperidine ring via a 1,6-hydrogen atom transfer. Starting from N-chloro-N-methylpentanamine, the reaction proceeds through an aminium radical which abstracts a hydrogen from the epsilon-carbon, leading to the formation of N-methylpiperidine after cyclization.\",\n      \"collision_avoidance_note\": \"This example showcases piperidine formation, a common HLF outcome. The clear N-haloamine to cyclic amine transformation without any Barton-associated functional groups (e.g., carboxylic acids, thioesters)"}], "role": "model"}, "finishReason": "MAX_TOKENS", "index": 0}], "usageMetadata": {"promptTokenCount": 1877, "candidate
