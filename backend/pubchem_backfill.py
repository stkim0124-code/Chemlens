import sqlite3
import requests
import time
import re

DB_PATH = 'app/labint.db'

SKIP = set([
    'TABLE OF CONTENTS', 'SEARCH TEXT', 'Abbreviation',
    'Chemical Name', 'Chemical Structure', 'NA', 'NA.',
    'O', 'N', 'S', 'H', 'Cl', 'Br', 'F', 'I', 'P', 'B',
])

def get_names(notes):
    result = []
    for ln in (notes or '').splitlines():
        ln = ln.strip()
        if not ln or ln in SKIP:
            continue
        if len(ln) < 5 or len(ln) > 80:
            continue
        if re.match(r'^[ivxlIVXL]+$', ln):
            continue
        if re.match(r'^[A-Z0-9]{1,8}$', ln):
            continue
        if re.match(r'^[A-Z0-9\s\-\(\)\/]+$', ln):
            continue
        if re.search(r'[a-z]{2,}', ln):
            result.append(ln)
    return result[:5]

def pubchem(name):
    try:
        url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
        url += requests.utils.quote(name.strip())
        url += "/property/IsomericSMILES/JSON"
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            data = r.json()
            props = data.get('PropertyTable', {}).get('Properties', [])
            if props:
                p = props[0]
                return p.get('IsomericSMILES') or p.get('SMILES') or p.get('CanonicalSMILES') or ''
    except Exception:
        pass
    return None

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

rows = conn.execute(
    "SELECT id, title, notes FROM reaction_cards "
    "WHERE (substrate_smiles = '' OR substrate_smiles IS NULL) "
    "AND title LIKE '%named reactions%' "
    "AND notes NOT LIKE '%(heading)%' "
    "AND length(notes) > 30 "
    "ORDER BY id"
).fetchall()

total = len(rows)
ok = 0
fail = 0
print("총 " + str(total) + "개 처리 시작")

for i, row in enumerate(rows):
    if i % 30 == 0:
        print("진행 " + str(i) + "/" + str(total) + " ok=" + str(ok))

    names = get_names(row['notes'])
    found = None

    for name in names:
        s = pubchem(name)
        if s:
            found = s
            print("OK id=" + str(row['id']) + " [" + name[:40] + "] " + s[:40])
            break
        time.sleep(0.1)

    if found:
        conn.execute(
            "UPDATE reaction_cards SET substrate_smiles=? WHERE id=?",
            (found, row['id'])
        )
        conn.commit()
        ok += 1
    else:
        fail += 1

print("완료 ok=" + str(ok) + " fail=" + str(fail))
conn.close()