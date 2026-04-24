"""Diff current evidence_search.py (122083) vs iter2_pre backup (124010)."""
import difflib, pathlib
cur = pathlib.Path(r'C:\chemlens\backend\app\evidence_search.py').read_text(encoding='utf-8').splitlines()
bak = pathlib.Path(r'C:\chemlens\backend\app\evidence_search.py.bak_gateB_iter2_pre_20260424_203111').read_text(encoding='utf-8').splitlines()
diff = list(difflib.unified_diff(cur, bak, fromfile='current_122083', tofile='iter2_pre_124010', lineterm='', n=1))
for line in diff[:200]:
    print(line)
print('---')
print('total diff lines:', len(diff))
