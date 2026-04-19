CHEMLENS backend repair patch (2026-04-12)

This patch fixes the concrete frontmatter/backend issues found in the uploaded backend.zip.

Included fixes
- Restored run_backend.bat (it was 0-byte broken)
- Added app/frontmatter_repair.py to normalize legacy frontmatter labels and dedupe aliases
- Patched app/labint_frontmatter_batch13.py to keep only page-verified source_page assignments
- Patched app/labint_frontmatter_batch_restore_utils.py so future batch re-apply is safe
- Expanded app/labint_frontmatter.py get_frontmatter_counts() so batch11~14 state is visible
- Added repair_labint_frontmatter_state.py to repair and reapply batch11~14 in one shot
- Repaired bundled DBs: app/labint.db and app/labint_round9_bridge_work.db

What this does NOT include
- No .env file (intentionally excluded)
- No node_modules equivalent / cache residue in the patch zip
