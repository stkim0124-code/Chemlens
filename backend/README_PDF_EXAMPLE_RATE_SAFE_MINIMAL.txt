PDF example automation minimal rate-safe patch

What changed
- Slowed down successful calls (default --sleep 20)
- Process only first region per page by default (--max-regions-per-page 1)
- Stop the run on the first HTTP 429 instead of hammering all remaining regions
- Added visible [CALL] / [OK] / [RATE LIMIT] console logs

Recommended smoke test
set GEMINI_API_KEY=YOUR_KEY
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction" --limit-pages 1

If that survives, then
python pdf_example_automation.py --backend-root . --call-gemini --families "Alkene (Olefin) Metathesis;Aldol Reaction" --limit-pages 2 --sleep 25 --max-regions-per-page 1 --cooldown-on-429 600
