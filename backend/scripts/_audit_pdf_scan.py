"""Step 1 of PDF vs CHEMLENS coverage audit — using pymupdf (fitz)."""
import json, re
from pathlib import Path

PDF_PATH = Path(r'C:\chemlens\backend\scripts\_audit_named_reactions.pdf')
OUT_DIR = Path(r'C:\chemlens\backend\scripts')

def main():
    import fitz  # pymupdf
    meta = {'pdf_path': str(PDF_PATH), 'pages': 0,
            'toc_outline': [],
            'toc_text_hits': [],
            'heading_candidates': []}
    all_text_lines = []

    doc = fitz.open(str(PDF_PATH))
    meta['pages'] = doc.page_count

    # Built-in TOC if present
    outline = doc.get_toc(simple=True)
    for lvl, title, page in outline:
        meta['toc_outline'].append({'level': lvl, 'title': title, 'page': page})

    for pi in range(doc.page_count):
        page = doc.load_page(pi)
        try:
            t = page.get_text("text") or ""
        except Exception as e:
            t = f"[ERROR page {pi+1}: {e}]"
        all_text_lines.append(f"=== PAGE {pi+1} ===")
        all_text_lines.append(t)

        # Heuristic: lines containing "Reaction"/"Rearrangement"/"Synthesis" and matching
        # a chapter-heading-ish pattern (short-ish, leading cap)
        for line in t.split("\n"):
            s = line.strip()
            if not s or len(s) > 100:
                continue
            # Numbered TOC-like
            if re.match(r"^\d+(\.\d+)?\.?\s+[A-Z][A-Za-z0-9 \-'’\(\)\-–,/&\.]{3,80}$", s):
                meta['toc_text_hits'].append({'page': pi+1, 'line': s})
            # Heading-like endings
            if re.match(r"^[A-Z][A-Za-z0-9 \-'’\(\)\-–,/&\.]{2,60}\s+(Reaction|Rearrangement|Synthesis|Oxidation|Reduction|Condensation|Coupling|Addition|Elimination|Olefination|Epoxidation|Cyclopropanation|Metathesis|Hydrogenation|Hydroformylation|Substitution|Esterification|Amidation|Cycloaddition|Annulation|Isomerization)s?\s*$", s):
                meta['heading_candidates'].append({'page': pi+1, 'line': s})

    doc.close()

    OUT_DIR.joinpath('_audit_pdf_fulltext.txt').write_text("\n".join(all_text_lines), encoding='utf-8')
    OUT_DIR.joinpath('_audit_pdf_metadata.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f"pages={meta['pages']}")
    print(f"toc_outline_entries={len(meta['toc_outline'])}")
    print(f"toc_text_hits={len(meta['toc_text_hits'])}")
    print(f"heading_candidates={len(meta['heading_candidates'])}")
    total_chars = sum(len(l) for l in all_text_lines)
    print(f"fulltext chars={total_chars:,}")
    # First 20 TOC entries
    if meta['toc_outline']:
        print("\n--- outline (first 30) ---")
        for i, e in enumerate(meta['toc_outline'][:30]):
            print(f"  L{e['level']} p{e['page']:>4}  {e['title']}")

if __name__ == '__main__':
    main()
