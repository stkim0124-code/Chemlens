Krapcho missing=1 close patch v2

This patch does NOT modify DB reaction evidence.
It only updates final_state_verifier.py and family_completion_dashboard.py so that:

- Krapcho Dealkoxycarbonylation
- Krapcho Reaction

are both collapsed to the canonical family:

- Krapcho Dealkoxycarbonylation (Krapcho Reaction)

Use this after phase15 completion when Krapcho evidence already exists but the canonicalized
missing count still shows 1 due to short-form alias drift.

Run:
  conda activate chemlens
  cd /d C:\chemlens\backend
  python final_state_verifier.py --db app\labint.db
  python family_completion_dashboard.py --backend-root .

Expected:
- completion_overview.missing_count -> 0
- missing_family_sample_canonicalized -> []
- Krapcho remains only in rich / recent completed views, not missing.
