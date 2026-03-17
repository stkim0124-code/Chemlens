import sqlite3 
conn = sqlite3.connect('app/labint.db') 
conn.execute("INSERT INTO reaction_cards (title,transformation,substrate_smiles,product_smiles,source,notes) VALUES ('benzene_test','test','c1ccccc1','c1ccccc1','manual','')") 
conn.commit() 
conn.close() 
print('done') 
