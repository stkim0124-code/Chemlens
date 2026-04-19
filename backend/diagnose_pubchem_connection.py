
from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

def _safe_import_versions():
    out = {}
    try:
        import requests
        out["requests"] = getattr(requests, "__version__", "unknown")
    except Exception as e:
        out["requests_error"] = repr(e)
    try:
        import urllib3
        out["urllib3"] = getattr(urllib3, "__version__", "unknown")
    except Exception as e:
        out["urllib3_error"] = repr(e)
    try:
        import charset_normalizer
        out["charset_normalizer"] = getattr(charset_normalizer, "__version__", "unknown")
    except Exception as e:
        out["charset_normalizer_error"] = repr(e)
    try:
        import chardet
        out["chardet"] = getattr(chardet, "__version__", "unknown")
    except Exception as e:
        out["chardet_error"] = repr(e)
    return out

def load_sample_names_from_db(db_path: Path, limit: int = 10) -> list[str]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT normalized_text
        FROM extract_molecules
        WHERE quality_tier = 3
          AND smiles IS NULL
          AND normalized_text IS NOT NULL
        ORDER BY id
        LIMIT ?
        """,
        (limit,),
    )
    rows = [r["normalized_text"] for r in cur.fetchall()]
    conn.close()
    return [str(x).strip() for x in rows if x]

def build_test_names(user_names: Iterable[str], db_names: Iterable[str]) -> list[str]:
    base = [
        "benzene",
        "ethanol",
        "acetic acid",
        "acetone",
        "pyridine",
    ]
    out = []
    seen = set()
    for name in list(base) + list(user_names) + list(db_names):
        n = str(name).strip()
        if not n or n in seen:
            continue
        out.append(n)
        seen.add(n)
    return out

def test_pubchem_name(name: str, timeout: int = 10) -> dict:
    import requests

    url = (
        "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
        f"{requests.utils.quote(name)}/property/IsomericSMILES/JSON"
    )
    result = {
        "name": name,
        "url": url,
    }
    try:
        r = requests.get(url, timeout=timeout)
        result["status_code"] = r.status_code
        result["ok"] = bool(r.ok)
        result["content_type"] = r.headers.get("content-type")
        text = r.text[:300].replace("\n", " ")
        result["body_preview"] = text
        if r.ok:
            try:
                data = r.json()
                props = data.get("PropertyTable", {}).get("Properties", [])
                if props:
                    result["isomeric_smiles"] = props[0].get("IsomericSMILES")
                    result["canonical_smiles"] = props[0].get("CanonicalSMILES")
            except Exception as e:
                result["json_error"] = repr(e)
    except Exception as e:
        result["exception"] = repr(e)
    return result

def test_pubchem_ping(timeout: int = 10) -> dict:
    import requests
    url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/benzene/property/IsomericSMILES/JSON"
    result = {"url": url}
    try:
        r = requests.get(url, timeout=timeout)
        result["status_code"] = r.status_code
        result["ok"] = bool(r.ok)
        result["content_type"] = r.headers.get("content-type")
        result["body_preview"] = r.text[:200].replace("\n", " ")
    except Exception as e:
        result["exception"] = repr(e)
    return result

def main():
    parser = argparse.ArgumentParser(description="Diagnose PubChem connectivity and name lookup behavior")
    parser.add_argument("--db", default="app/labint.db", help="Optional DB path for sampling tier3 names")
    parser.add_argument("--name", action="append", default=[], help="Additional test name(s)")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--sample-limit", type=int, default=8)
    args = parser.parse_args()

    db_path = Path(args.db)

    print("=== Environment ===")
    print(f"python: {sys.version}")
    print(f"platform: {platform.platform()}")
    versions = _safe_import_versions()
    for k, v in versions.items():
        print(f"{k}: {v}")

    print("\n=== PubChem Ping Test ===")
    ping = test_pubchem_ping(timeout=args.timeout)
    print(json.dumps(ping, ensure_ascii=False, indent=2))

    db_names = load_sample_names_from_db(db_path, limit=args.sample_limit)
    test_names = build_test_names(args.name, db_names)

    print("\n=== Test Names ===")
    for i, name in enumerate(test_names, 1):
        print(f"{i:02d}. {name}")

    print("\n=== Lookup Results ===")
    success = 0
    fail = 0
    for name in test_names:
        result = test_pubchem_name(name, timeout=args.timeout)
        ok = bool(result.get("ok")) and bool(result.get("isomeric_smiles") or result.get("canonical_smiles"))
        if ok:
            success += 1
        else:
            fail += 1
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("-" * 80)

    print("\n=== Summary ===")
    print(f"success: {success}")
    print(f"fail: {fail}")

    if success == 0:
        print("diagnosis: PubChem connectivity / HTTP / SSL / environment issue is highly likely.")
    elif success < max(1, len(test_names) // 4):
        print("diagnosis: Connectivity works partially, but many names are unsuitable or responses are unstable.")
    else:
        print("diagnosis: Connectivity is working; tier3 filtering / name quality is the main next target.")

if __name__ == "__main__":
    main()
