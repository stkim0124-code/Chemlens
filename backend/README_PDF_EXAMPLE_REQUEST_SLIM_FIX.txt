This patch fixes the request-slim experiment so it actually uses the lean payload builder.

Changes:
- gemini_call now uses build_lean_user_text(...)
- prints [PAYLOAD] prompt_chars per call
- cooldown-on-429 default is 0s, so the run stops immediately instead of sleeping 300s
- [RATE LIMIT] now prints the actual exception text

Recommended smoke test:
set GEMINI_API_KEY=YOUR_KEY
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction" --limit-pages 1
