"""Check Macrolactonization + Ullmann family labels in labint.db (Gate C umbrella decision)."""
import sqlite3

conn = sqlite3.connect(r'C:\chemlens\backend\app\labint.db')
cur = conn.cursor()
print('=== Tables ===')
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
for t in tables:
    print(' ', t)

# Find likely family-carrying tables
for t in tables:
    try:
        cur.execute(f"PRAGMA table_info({t})")
        cols = [c[1] for c in cur.fetchall()]
        if any('family' in c.lower() for c in cols) or any('reaction' in c.lower() for c in cols):
            print(f'\n--- {t} cols: {cols}')
    except Exception as e:
        pass

# Search across tables for macrolactonization / ullmann labels
for t in tables:
    try:
        cur.execute(f"PRAGMA table_info({t})")
        cols = [c[1] for c in cur.fetchall()]
        fam_col = None
        for c in cols:
            if c.lower() in ('family', 'family_name', 'reaction_family', 'named_reaction', 'reaction_name'):
                fam_col = c
                break
        if fam_col:
            cur.execute(f"SELECT DISTINCT {fam_col}, COUNT(*) FROM {t} WHERE {fam_col} LIKE '%acrolacton%' OR {fam_col} LIKE '%llmann%' GROUP BY {fam_col} ORDER BY 2 DESC")
            rows = cur.fetchall()
            if rows:
                print(f'\n=== {t}.{fam_col} matches ===')
                for r in rows:
                    print(f'  {r[1]:5d}  {r[0]!r}')
    except Exception as e:
        print(f'{t}: {e}')
