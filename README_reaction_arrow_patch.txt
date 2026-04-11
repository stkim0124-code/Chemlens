CHEMLENS reaction arrow patch

What changed
- Re-enabled Ketcher reaction/arrow UI in frontend/public/ketcher/index.html
- App.jsx now auto-detects reaction SMILES (contains >>)
- Reaction queries no longer fall back to product-only search
- Backend evidence_search now accepts reaction_smiles and searches reactant/agent/product components together
- Evidence panel shows reaction mode, reaction components, and matched components per hit

Apply
1) Extract this ZIP at C:\chemlens and overwrite existing files.
2) Restart backend.
3) Restart frontend.

How to use
- Draw reactants on the left, products on the right, and place a reaction arrow in Ketcher.
- Click: Apply (분자/반응식 자동 감지 → 검색)
- The evidence panel will run reaction-aware search using reactant/agent/product components.

Notes
- This makes arrows useful for evidence search, not just drawing.
- Direct reaction-card search still remains molecule-oriented; reaction arrows currently enhance the Named Reaction Evidence panel.
- Result quality is still limited by how many exact/generic structures exist in extract_molecules.
