CHEMLENS Step 1 Patch - family-level aggregation / dedup

Overwrite these files into your project root:
- backend/app/evidence_search.py
- frontend/src/components/EvidencePanel.jsx

What changed:
1) Backend now groups raw extract hits by reaction_family_name.
2) Final result returns one representative card per family.
3) Additional evidence from the same family is kept inside family_evidence_items.
4) Same-family fallback cards are suppressed at this stage to avoid duplicate cards.
5) Frontend now shows family group count, collapsed evidence count,
   and additional same-family evidence inside the details block.
