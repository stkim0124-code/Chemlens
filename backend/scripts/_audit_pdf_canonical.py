"""Step 1b — from the PDF outline, extract the clean canonical list of named reactions.

Output:
  _audit_pdf_canonical.json — {"entries": [{"raw": ..., "clean": ..., "aliases": [...], "page": N}]}
"""
import json, re
from pathlib import Path

META = Path(r'C:\chemlens\backend\scripts\_audit_pdf_metadata.json')
OUT  = Path(r'C:\chemlens\backend\scripts\_audit_pdf_canonical.json')

def normalize(s: str) -> str:
    """Title-case with small caps for connectors."""
    s = s.strip()
    # Fix common OCR typos seen in the outline
    fixes = {
        'Synhesis': 'Synthesis',
        'Forword': 'Foreword',
        'Rearragement': 'Rearrangement',
        'Rearrangment': 'Rearrangement',
        'Cyclopropanantion': 'Cyclopropanation',
    }
    for wrong, right in fixes.items():
        s = s.replace(wrong, right)
    # Collapse multiple spaces / newlines
    s = re.sub(r'\s+', ' ', s)
    # Title-case everything that's ALL CAPS (like "ARBUZOV REACTION")
    def _tc(token: str) -> str:
        # Preserve small connectors
        if token.lower() in {'and','or','of','the','to','in','with','for','by','via','a','an'}:
            return token.lower()
        # Preserve Roman numerals up to X
        if re.match(r'^[IVX]+$', token):
            return token.upper()
        # Preserve chemical formulas like CH3, NH2, tBu (must contain a digit OR be mixed case)
        if re.match(r'^[A-Z]+\d+[A-Za-z]*\d*$', token) or re.match(r'^[a-z][A-Z][a-z]*$', token):
            return token  # leave as-is
        return token.capitalize()
    # Title case ANY string where upper chars outnumber lower chars
    if sum(c.isupper() for c in s) > sum(c.islower() for c in s):
        # Split on word-boundaries keeping separators
        tokens = re.findall(r"[A-Za-z0-9]+|[^A-Za-z0-9]+", s)
        out = []
        for tok in tokens:
            if tok.isalpha() or (tok and tok[0].isalpha()):
                out.append(_tc(tok))
            else:
                out.append(tok)
        s = ''.join(out)
        # Capitalize the first character
        if s:
            s = s[0].upper() + s[1:]
    # Clean odd mojibake glyph from outline (Baldwin's)
    s = s.replace('\ufffd\ufffdS', "'s").replace('\ufffd\ufffd', "'")
    # Fix possessive 'S -> 's (Baldwin'S -> Baldwin's) after title-case pass
    s = re.sub(r"(\u2019|')S\b", lambda m: m.group(1) + 's', s)
    return s.strip()

def split_aliases(clean: str):
    """If 'Foo Reaction (Bar Reaction)' → aliases = ['Foo Reaction', 'Bar Reaction']."""
    m = re.match(r'^(.+?)\s*\((.+?)\)\s*$', clean)
    if m:
        return [m.group(1).strip(), m.group(2).strip()]
    return [clean.strip()]

def main():
    meta = json.loads(META.read_text(encoding='utf-8'))
    outline = meta.get('toc_outline', [])
    # VII section starts at p53; only L3 entries inside VII count.
    # VIII etc. come after — stop when level drops to L1/L2 in back matter.
    entries = []
    in_section_VII = False
    for e in outline:
        lvl, title, page = e['level'], e['title'], e['page']
        t = (title or '').strip()
        if not t:
            continue
        if lvl in (1, 2):
            if 'Named Organic Reactions' in t and 'Alphabetical' in t:
                in_section_VII = True
                continue
            elif in_section_VII and re.match(r'^(VIII\.|IX\.|X\.|Appendix|Index|References|Bibliography)', t, re.I):
                # left the section
                in_section_VII = False
        if in_section_VII and lvl == 3:
            clean = normalize(t)
            # Skip purely numeric entries ("5", "6", "7", "8") which are sub-pages
            if re.match(r'^\d+\s*$', clean):
                continue
            # Skip single-char / very short
            if len(clean) < 4:
                continue
            aliases = split_aliases(clean)
            entries.append({'raw': t, 'clean': clean, 'aliases': aliases, 'page': page})

    # Also add L4 entries if any (sub-variants of reactions)
    # (For this PDF, likely none — 547 outline rows is mostly L3 under VII)

    OUT.write_text(json.dumps({'count': len(entries), 'entries': entries},
                              ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"canonical_entries={len(entries)}")
    print("\n--- first 20 ---")
    for e in entries[:20]:
        print(f"  p{e['page']:>4}  {e['clean']}")
    print("\n--- last 10 ---")
    for e in entries[-10:]:
        print(f"  p{e['page']:>4}  {e['clean']}")

if __name__ == '__main__':
    main()
