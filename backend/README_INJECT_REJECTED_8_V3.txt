Inject rejected 8 families v3

What changed in v3:
- Fixes stage/canonical PRAGMA column-name mapping bug
- Preserves extract_molecules.role from stage rows
- Falls back to role='unknown' only if stage role is missing
- DRY-RUN now performs actual insert rehearsal inside a transaction and rolls back
  so NOT NULL/FK/schema mismatches appear before APPLY

Usage:
1) Unzip into C:\chemlens\backend and overwrite:
   - inject_rejected_8_families.py
   - run_inject_rejected_8_families.bat
2) Run:
   conda activate chemlens
   cd /d C:\chemlens\backend
   run_inject_rejected_8_families.bat

Expected DRY-RUN output in v3:
- READY with roles for each family, OR a concrete insert_rehearsal_error
Expected APPLY output in v3:
- no more NOT NULL constraint failed: extract_molecules.role
