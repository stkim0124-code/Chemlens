# ChemLens v5 patch notes

## Why v5 exists
- v3 proved Gemini discovery can create real named-reaction evidence.
- v4.5 proved discovery frontier control can add families safely from a zero-seed frontier.
- v5 combines both: discovery-first engine + frontier-aware controller.

## Main differences from v4.5
- Adds auto mode selection (`frontier_discovery`, `seed_assisted_bulk`, `bulk_discovery`)
- Adds adaptive frontier growth (3 -> 4 -> 5 -> 6 after repeated full-success rounds)
- Adds periodic diagnostic benchmark runs during long runs
- Tightens Gemini family mismatch handling
- Moves default report root to `reports/v5`

## Safety properties kept
- SQL DELETE rollback only
- no candidate backup DB files
- bounded snapshots only
- Windows-safe `.env` reading and benchmark subprocess handling
