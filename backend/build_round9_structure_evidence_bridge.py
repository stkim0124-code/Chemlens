from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import requests as _requests
except ImportError:
    _requests = None  # type: ignore

from app.labint_intel import backfill_labint_intel, ensure_labint_intel_schema

try:
    from rdkit import Chem
    from rdkit import RDLogger
except Exception as e:  # pragma: no cover
    Chem = None  # type: ignore
    RDLogger = None  # type: ignore
    _RDKIT_ERR = str(e)
else:
    _RDKIT_ERR = None
    RDLogger.DisableLog("rdApp.*")


BACKEND_DIR = Path(__file__).resolve().parent
APP_DIR = BACKEND_DIR / "app"

DEFAULT_MAIN_DB = APP_DIR / "labint.db"
DEFAULT_STAGING_DB = APP_DIR / "labint_round9_v5_final_staging.db"
DEFAULT_OUT_DB = APP_DIR / "labint_bridge_work.db"

TOKEN_RE = re.compile(r"(?<![A-Za-z0-9])([A-Za-z0-9@+\-\[\]\(\)=#$\\/\.%:]{3,})(?![A-Za-z0-9])")

# ── PubChem fallback 설정 ─────────────────────────────────────────────────
PUBCHEM_URL = (
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
    "{name}/property/IsomericSMILES/JSON"
)
PUBCHEM_DELAY = 0.35      # 초 (3 req/s 이하 유지)
PUBCHEM_TIMEOUT = 10      # 초
PUBCHEM_ENABLED = True    # False로 설정하면 PubChem 조회 건너뜀

# PubChem 호출 캐시 (프로세스 내 중복 API 호출 방지)
_pubchem_cache: dict[str, Optional[str]] = {}

# PubChem fallback에서 제외할 플레이스홀더/일반명
_PUBCHEM_SKIP_EXACT: set[str] = {
    "reactant","product","reagent","solvent","intermediate","catalyst",
    "substrate","electrophile","nucleophile","base","acid","heat","water",
    "alkene","alkyne","ketone","aldehyde","alcohol","amine","ester","alkyl",
    "aryl","cyclic","linear","bicyclic","imide","imine","enol","enone",
    "diene","dienophile","acyloin","enediyne","nitrone",
    "Reactant","Product","Reagent","Solvent","Intermediate","Catalyst",
    "Alkene","Alkane","Alkyne","Benzene","Nitrile","Ketone","Electrophile",
    "toluene","benzene","pyridine","methanol","ethanol","acetone",
    "THF","DMF","DMSO","DCM","xylene","dioxane","diglyme",
    "quinoline","indole","pyrrole","oxime","diazoketone",
    "Isonitrile","Lindlar's catalyst","Burgess reagent",
    "enolate","dianion","zinc carbenoid","carbanion","carbenoid",
}
_PUBCHEM_SKIP_RE: list = [
    re.compile(r"\bR\d*\b|\bAr\b(?!\w)|\bX\b(?!\w)|\bZ\b|\bLG\b|\bNu\b"),
    re.compile(r";"),
    re.compile(r"\d+\s*(equiv|mol%|°C|min|hr|h\b|wt%|mmol|mL)"),
    re.compile(r"^(heat|hv|Δ|RT|reflux|\d|:|°|\[)", re.I),
    re.compile(r"\b(enolate|dianion|carbene|ylide|radical|skeleton|system)\b", re.I),
    re.compile(r"\b(intermediate|precursor|derivative|analog|polymer)\b", re.I),
    re.compile(r"\b(mixture|salt\b|complex\b|product\b|substrate\b)\b", re.I),
    re.compile(r"\b(then|followed|step|excess|cat\.\b)\b", re.I),
    re.compile(r"[,]\s*(or|and)\s+[a-z]", re.I),
]


def _is_pubchem_name(text: str) -> bool:
    """PubChem 이름 조회 가치가 있는 화학명인지 판단."""
    t = text.strip()
    if len(t) < 5 or len(t) > 80:
        return False
    if t in _PUBCHEM_SKIP_EXACT:
        return False
    if not re.search(r"[a-z]{3,}", t):
        return False
    if t.count(",") > 1:
        return False
    for pat in _PUBCHEM_SKIP_RE:
        if pat.search(t):
            return False
    return True


def _pubchem_lookup(name: str) -> Optional[str]:
    """
    화학명 → canonical SMILES. 캐시 사용.
    - 404 / no-hit → None 캐시 (영구 미스로 처리)
    - 429 / 5xx   → backoff 재시도 최대 2회, 실패 시 None 비캐시 (일시 오류)
    - exception   → None 비캐시 (일시 오류)
    rate limit: 호출자가 PUBCHEM_DELAY 슬립 책임.
    """
    if _requests is None or not PUBCHEM_ENABLED:
        return None
    if name in _pubchem_cache:
        return _pubchem_cache[name]
    url = PUBCHEM_URL.format(name=_requests.utils.quote(name.strip()))
    for attempt in range(3):
        try:
            r = _requests.get(url, timeout=PUBCHEM_TIMEOUT)
            if r.status_code == 200:
                props = r.json().get("PropertyTable", {}).get("Properties", [])
                if props:
                    p = props[0]
                    raw = (p.get("IsomericSMILES") or
                           p.get("CanonicalSMILES") or
                           p.get("SMILES"))
                    if raw and Chem is not None:
                        mol = Chem.MolFromSmiles(raw)
                        if mol:
                            canon = Chem.MolToSmiles(mol, canonical=True)
                            _pubchem_cache[name] = canon   # 영구 캐시
                            return canon
                _pubchem_cache[name] = None                # no-hit 영구 캐시
                return None
            if r.status_code == 404:
                _pubchem_cache[name] = None                # no-hit 영구 캐시
                return None
            if r.status_code in (429, 500, 503):
                wait = PUBCHEM_DELAY * (3 ** attempt)
                time.sleep(wait)
                continue                                    # 재시도 (캐시 안 씀)
            _pubchem_cache[name] = None                    # 기타 오류 영구 캐시
            return None
        except Exception:
            if attempt < 2:
                time.sleep(PUBCHEM_DELAY * 2)
                continue
            return None                                     # 일시 오류 비캐시
    return None


def _require_rdkit() -> None:
    if Chem is None:
        raise RuntimeError(f"RDKit is required to build the bridge DB. {_RDKIT_ERR}")


def candidate_strings(text: str) -> List[str]:
    if not text:
        return []
    items: List[str] = []
    for part in re.split(r"[;\n]", text):
        part = part.strip()
        if not part:
            continue
        items.append(part)
        if "|" in part:
            items.extend([s.strip() for s in part.split("|") if s.strip()])
        for tok in TOKEN_RE.findall(part):
            if len(tok) >= 3 and (any(ch in tok for ch in "[]=#@\\/()[]") or bool(re.search(r"\d", tok))):
                items.append(tok)
    seen = set()
    out: List[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def classify_candidate(candidate: str) -> Tuple[int, str, str]:
    """
    tier 분류:
      1 = exact SMILES (RDKit direct parse 또는 PubChem name lookup)
      2 = SMARTS generic pattern
      3 = 미분류 (이름 기반이지만 구조 미확보)

    반환값: (tier, normalized_smiles_or_smarts, source_kind)
      source_kind:
        "text_smiles"         — RDKit direct parse 성공
        "text_smarts"         — SMARTS generic pattern
        "pubchem_name_lookup" — PubChem API 조회 성공
        "text_only"           — 미분류

    PubChem fallback:
      RDKit/SMARTS 파싱이 모두 실패하고 _is_pubchem_name() 통과 시
      PubChem API 조회. 성공하면 tier1 반환.
      rate limit: 캐시 미스 시만 PUBCHEM_DELAY 슬립.
    """
    if not candidate:
        return 3, "", "text_only"
    _require_rdkit()

    # 1순위: RDKit direct SMILES parse
    mol = Chem.MolFromSmiles(candidate)
    if mol is not None:
        return 1, Chem.MolToSmiles(mol, canonical=True), "text_smiles"

    # 2순위: SMARTS generic pattern
    qmol = Chem.MolFromSmarts(candidate)
    if qmol is not None:
        return 2, candidate, "text_smarts"

    # 3순위: PubChem name lookup (화학명 후보인 경우에만)
    if _is_pubchem_name(candidate):
        cached = candidate in _pubchem_cache
        result = _pubchem_lookup(candidate)
        if not cached:
            time.sleep(PUBCHEM_DELAY)  # rate limit (캐시 미스 시만)
        if result:
            return 1, result, "pubchem_name_lookup"

    return 3, "", "text_only"


def copy_table_sql(staging_conn: sqlite3.Connection, work_conn: sqlite3.Connection, table_name: str) -> None:
    create_sql = staging_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone()
    if not create_sql or not create_sql[0]:
        raise RuntimeError(f"Missing schema for {table_name} in staging DB")
    work_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    work_conn.execute(create_sql[0])
    rows = staging_conn.execute(f"SELECT * FROM {table_name}").fetchall()
    if rows:
        placeholders = ",".join(["?"] * len(rows[0]))
        work_conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)


def ensure_extract_molecules_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS extract_molecules")
    conn.execute(
        """
        CREATE TABLE extract_molecules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extract_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            smiles TEXT,
            smiles_kind TEXT,
            quality_tier INTEGER NOT NULL DEFAULT 3,
            reaction_family_name TEXT,
            source_zip TEXT,
            page_no INTEGER,
            queryable INTEGER NOT NULL DEFAULT 0,
            note_text TEXT,
            morgan_fp BLOB,
            normalized_text TEXT,
            source_field TEXT,
            structure_source TEXT,
            alias_id INTEGER,
            fg_tags TEXT,
            role_confidence REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_extract_id ON extract_molecules(extract_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_tier_queryable ON extract_molecules(quality_tier, queryable)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_family ON extract_molecules(reaction_family_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_extract_molecules_page ON extract_molecules(source_zip, page_no)")


def backfill_extract_molecules(conn: sqlite3.Connection) -> dict:
    _require_rdkit()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            re.id AS extract_id,
            re.reaction_family_name,
            re.reactants_text,
            re.products_text,
            pi.source_zip,
            pi.page_no
        FROM reaction_extracts re
        JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
        JOIN page_images pi ON sc.page_image_id = pi.id
        ORDER BY re.id
        """
    ).fetchall()

    inserted = 0
    tier_counts = {1: 0, 2: 0, 3: 0}
    for row in rows:
        extract_id, family, reactants_text, products_text, source_zip, page_no = row
        for role, text in (("reactant", reactants_text or ""), ("product", products_text or "")):
            text = text.strip()
            if not text:
                continue
            parsed_any = False
            seen_local = set()
            for cand in candidate_strings(text):
                tier, normalized, source_kind = classify_candidate(cand)
                if tier in (1, 2) and normalized and (role, normalized) not in seen_local:
                    seen_local.add((role, normalized))
                    # role_confidence: PubChem 유래는 0.80, direct/smarts는 0.92/0.75
                    if source_kind == "pubchem_name_lookup":
                        role_conf = 0.80
                    elif role in {"reactant", "product"}:
                        role_conf = 0.92
                    else:
                        role_conf = 0.75
                    cur.execute(
                        """
                        INSERT INTO extract_molecules
                        (extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                         source_zip, page_no, queryable, note_text, morgan_fp,
                         normalized_text, source_field, structure_source, alias_id, fg_tags, role_confidence)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, ?)
                        """,
                        (
                            extract_id,
                            role,
                            normalized,
                            "exact" if tier == 1 else "generic",
                            tier,
                            family,
                            source_zip,
                            page_no,
                            1,
                            text,
                            text,
                            "reactants_text" if role == "reactant" else "products_text",
                            source_kind,   # provenance 보존
                            role_conf,
                        ),
                    )
                    inserted += 1
                    tier_counts[tier] += 1
                    parsed_any = True
            if not parsed_any:
                cur.execute(
                    """
                    INSERT INTO extract_molecules
                    (extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                     source_zip, page_no, queryable, note_text, morgan_fp,
                     normalized_text, source_field, structure_source, alias_id, fg_tags, role_confidence)
                    VALUES (?, ?, NULL, 'text_only', 3, ?, ?, ?, 0, ?, NULL, ?, ?, 'text_only', NULL, NULL, ?)
                    """,
                    (
                        extract_id,
                        role,
                        family,
                        source_zip,
                        page_no,
                        text,
                        text,
                        "reactants_text" if role == "reactant" else "products_text",
                        0.92 if role in {"reactant", "product"} else 0.75,
                    ),
                )
                inserted += 1
                tier_counts[3] += 1
    conn.commit()
    return {"inserted": inserted, "tier1": tier_counts[1], "tier2": tier_counts[2], "tier3": tier_counts[3]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build round9 structure-evidence bridge work DB")
    parser.add_argument("--main-db", default=str(DEFAULT_MAIN_DB))
    parser.add_argument("--staging-db", default=str(DEFAULT_STAGING_DB))
    parser.add_argument("--out-db", default=str(DEFAULT_OUT_DB))
    args = parser.parse_args()

    main_db = Path(args.main_db)
    staging_db = Path(args.staging_db)
    out_db = Path(args.out_db)

    print("[INFO] Canonical DB baseline: app/labint.db")
    print(f"[INFO] Disposable bridge work DB: {out_db}")

    if not main_db.exists():
        raise SystemExit(f"Main DB not found: {main_db}")
    if not staging_db.exists():
        raise SystemExit(f"Staging DB not found: {staging_db}")
    if out_db.resolve() == main_db.resolve():
        raise SystemExit("Safety: --out-db must not equal --main-db. "
                         "Use a separate disposable path (e.g. app/labint_bridge_work.db)")

    shutil.copy2(main_db, out_db)
    work_conn = sqlite3.connect(str(out_db))
    staging_conn = sqlite3.connect(str(staging_db))

    try:
        for table in ("page_images", "scheme_candidates", "reaction_extracts"):
            copy_table_sql(staging_conn, work_conn, table)
        ensure_extract_molecules_schema(work_conn)
        stats = backfill_extract_molecules(work_conn)
        ensure_labint_intel_schema(out_db)
        intel_stats = backfill_labint_intel(out_db)
        print("Bridge DB built:", out_db)
        print(stats)
        print({"intel": intel_stats})
        total_pages = work_conn.execute("SELECT COUNT(*) FROM page_images").fetchone()[0]
        total_schemes = work_conn.execute("SELECT COUNT(*) FROM scheme_candidates").fetchone()[0]
        total_extracts = work_conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0]
        print({"page_images": total_pages, "scheme_candidates": total_schemes, "reaction_extracts": total_extracts})
    finally:
        work_conn.close()
        staging_conn.close()


if __name__ == "__main__":
    main()
