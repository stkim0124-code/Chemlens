# backend/app/ingest_utils.py
from __future__ import annotations

import os
import re
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

os.environ.setdefault("PYTHONUTF8", "1")

IMAGE_EXTS_DEFAULT = (".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff", ".bmp")
PDF_EXTS_DEFAULT = (".pdf",)

# ------------------------------------------------------------
# FS helpers (Unicode-safe; Korean paths OK)
# ------------------------------------------------------------
def ensure_dir(p: str | Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def iter_files_recursive(root: str | Path) -> Iterator[Path]:
    root = Path(root)
    if not root.exists():
        return iter(())
    for p in root.rglob("*"):
        if p.is_file():
            yield p

def iter_image_files_recursive(
    root: str | Path,
    exts: Sequence[str] = IMAGE_EXTS_DEFAULT,
    limit: int = 0,
) -> List[Path]:
    exts_set = set(e.lower() if e.startswith(".") else "." + e.lower() for e in exts)
    out: List[Path] = []
    for p in iter_files_recursive(root):
        if p.suffix.lower() in exts_set:
            out.append(p)
            if limit and len(out) >= limit:
                break
    return out

def sha256_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()

def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def rel_top_folder_tag(root: str | Path, file_path: str | Path) -> str:
    root = Path(root).resolve()
    fp = Path(file_path).resolve()
    try:
        rel = fp.relative_to(root)
        if len(rel.parts) >= 2:
            return rel.parts[0]
        return ""
    except Exception:
        return ""

# ------------------------------------------------------------
# Card schema expected by app.main
# ------------------------------------------------------------
@dataclass
class ExtractedCard:
    title: str
    transformation: str = ""
    substrate_smiles: str = ""
    product_smiles: str = ""
    reagents: str = ""
    solvent: str = ""
    conditions: str = ""
    yield_pct: Optional[float] = None
    source: str = ""
    notes: str = ""

    # extra provenance
    doc_title: str = ""
    page_no: int = 0

# ------------------------------------------------------------
# Text extraction helpers
# ------------------------------------------------------------
def extract_procedure_blocks(text: str) -> List[str]:
    """Split to paragraph-like blocks and filter for procedure-ish blocks.
    If nothing matches, return all blocks.
    """
    if not text:
        return []
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", t) if b.strip()]
    if not blocks:
        return []

    cues = [
        r"\b(add|added|stir|stirred|heat|heated|cool|cooled|reflux|quenched|extract|washed|dried|filtered|concentrated|purified)\b",
        r"\b(yield|isolated|afforded|obtained)\b",
        r"(가하였다|가한다|첨가|가열|냉각|교반|환류|추출|세척|건조|여과|농축|정제|수득|얻었다|생성)",
        r"(\b\d+(\.\d+)?\s?(?:mg|g|kg|mL|L|mol|mmol|µL|%|°C|K|h|min)\b)",
    ]

    def looks_like_procedure(b: str) -> bool:
        return any(re.search(c, b, flags=re.IGNORECASE) for c in cues)

    proc = [b for b in blocks if looks_like_procedure(b)]
    return proc if proc else blocks

def extract_concept_headings(text: str) -> List[str]:
    """Extract heading-like lines (reaction names / section titles)."""
    if not text:
        return []
    lines = [ln.strip() for ln in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    out: List[str] = []
    for ln in lines:
        if not ln:
            continue
        # Heuristics: short-ish line without trailing period, often Title Case / has arrows etc.
        if len(ln) > 120:
            continue
        if re.search(r"^\d+(\.\d+)*\s+", ln):
            # numbered headings ok
            out.append(ln)
            continue
        if re.search(r"(reaction|synthesis|preparation|general procedure|실험|합성|제조|반응)", ln, flags=re.I):
            out.append(ln)
            continue
        if re.search(r"→|->|⇒|⟶|⟶", ln):
            out.append(ln)
            continue
    # de-dup preserve order
    seen=set()
    ded=[]
    for x in out:
        if x in seen: 
            continue
        seen.add(x); ded.append(x)
    return ded

# ------------------------------------------------------------
# Lightweight extraction from a block
# ------------------------------------------------------------
_RE_YIELD = re.compile(r"(?:yield|수득|수율)\s*[:\-]?\s*(\d{1,3}(?:\.\d+)?)\s*%?", re.I)
_RE_TEMP = re.compile(r"(-?\d{1,3})\s*°\s*C", re.I)
_RE_TIME = re.compile(r"(\d+(?:\.\d+)?)\s*(h|hr|hrs|hour|hours|min|mins|minute|minutes|시간|분)", re.I)

def _extract_yield(block: str) -> Optional[float]:
    m = _RE_YIELD.search(block or "")
    if not m:
        return None
    try:
        y=float(m.group(1))
        if 0 <= y <= 100:
            return y
    except Exception:
        return None
    return None

def _extract_conditions(block: str) -> str:
    temps = [m.group(0) for m in _RE_TEMP.finditer(block or "")]
    times = [m.group(0) for m in _RE_TIME.finditer(block or "")]
    parts = []
    if temps:
        parts.append("Temp: " + ", ".join(sorted(set(temps))))
    if times:
        parts.append("Time: " + ", ".join(sorted(set(times))))
    return " | ".join(parts)

# ------------------------------------------------------------
# SMILES extraction (best-effort)
# ------------------------------------------------------------
_SMILES_TOKEN = re.compile(r"\b([A-Za-z0-9@+\-\[\]\(\)=#$\\/\.]{6,})\b")
_SMILES_HINT = re.compile(r"(smiles|smi)\s*[:=]", re.I)

def extract_smiles_candidates(text: str, max_n: int = 20) -> List[str]:
    if not text:
        return []
    cands: List[str] = []
    # prioritize lines with SMILES hint
    for ln in text.splitlines():
        if _SMILES_HINT.search(ln):
            for tok in _SMILES_TOKEN.findall(ln):
                cands.append(tok)
    # fallback: scan whole text for tokens but keep small
    if len(cands) < 3:
        for tok in _SMILES_TOKEN.findall(text):
            cands.append(tok)
            if len(cands) >= max_n:
                break
    # de-dup
    out=[]
    seen=set()
    for s in cands:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= max_n:
            break
    return out

def pick_valid_smiles(cands: List[str], rdkit_available: bool = False) -> str:
    """Return first RDKit-validated SMILES if possible, else empty."""
    if not cands:
        return ""
    if not rdkit_available:
        return ""
    try:
        from rdkit import Chem  # type: ignore
    except Exception:
        return ""
    for s in cands:
        # quick reject: too many digits only etc.
        if len(s) < 6:
            continue
        m = Chem.MolFromSmiles(s)
        if m:
            return Chem.MolToSmiles(m, canonical=True)
    return ""

# ------------------------------------------------------------
# Builders expected by app.main:
#   build_procedure_cards(doc_title, page_no, blocks) -> List[ExtractedCard]
#   build_concept_cards(doc_title, page_no, headings) -> List[ExtractedCard]
# ------------------------------------------------------------
def build_procedure_cards(
    doc_title: str,
    page_no: int,
    blocks: List[str],
    rdkit_available: bool = False,
) -> List[ExtractedCard]:
    cards: List[ExtractedCard] = []
    for i, blk in enumerate(blocks or [], start=1):
        y = _extract_yield(blk)
        cond = _extract_conditions(blk)
        title = f"{doc_title} p{page_no+1} proc#{i}"
        cands = extract_smiles_candidates(blk)
        smiles = pick_valid_smiles(cands, rdkit_available=rdkit_available)
        cards.append(
            ExtractedCard(
                title=title,
                transformation="",
                substrate_smiles=smiles,
                reagents="",
                solvent="",
                conditions=cond,
                yield_pct=y,
                source=f"{doc_title}#p{page_no+1}",
                notes=blk[:2000],
                doc_title=doc_title,
                page_no=page_no,
            )
        )
    return cards

def build_concept_cards(
    doc_title: str,
    page_no: int,
    headings: List[str],
    rdkit_available: bool = False,
) -> List[ExtractedCard]:
    cards: List[ExtractedCard] = []
    for i, h in enumerate(headings or [], start=1):
        title = h.strip()
        if not title:
            continue
        cands = extract_smiles_candidates(h)
        smiles = pick_valid_smiles(cands, rdkit_available=rdkit_available)
        cards.append(
            ExtractedCard(
                title=title[:300],
                transformation="",
                substrate_smiles=smiles,
                source=f"{doc_title}#p{page_no+1}",
                notes=f"(heading) {h[:2000]}",
                doc_title=doc_title,
                page_no=page_no,
            )
        )
    return cards

# Backward-compatible aliases
walk_images = iter_image_files_recursive
walk_files = iter_files_recursive
