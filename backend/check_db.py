import sqlite3

conn = sqlite3.connect('app/labint.db')

print('=== named reactions 100페이지 이후 샘플 ===')
rows = conn.execute("""
    SELECT id, title, notes FROM reaction_cards 
    WHERE (substrate_smiles = '' OR substrate_smiles IS NULL)
    AND title LIKE '%named reactions%'
    ORDER BY id
    LIMIT 20 OFFSET 100
""").fetchall()

for r in rows:
    print(f'ID: {r[0]}')
    print(f'Title: {r[1][:80]}')
    print(f'Notes: {r[2][:400]}')
    print('---')

conn.close()