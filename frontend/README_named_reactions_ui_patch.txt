
FRONTEND PATCH NOTES

What changed
- Added a new tab: 🧪 Named Reactions
- The new tab calls:
  - GET /api/named-reactions/summary
  - GET /api/named-reactions/search
- This is a test UI for the named reactions.zip evidence layer only

What to verify
- Summary shows merged=true and counts for page_images / scheme_candidates / reaction_extracts
- Search for "Aldol Reaction" returns hits
- Search for "Claisen" returns hits
- Search for "Grubbs catalyst" returns alkene metathesis related hits
