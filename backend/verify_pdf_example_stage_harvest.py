from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path
from datetime import datetime

DEFAULT_STAGE_DB = Path("app") / "labint_pdf_examples_stage.db"
DEFAULT_REPORTS = Path("reports") / "pdf_example_stage_verification"


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def fetch_scalar(cur, sql, params=()):
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--backend-root", default=".")
    p.add_argument("--stage-db", type=Path, default=DEFAULT_STAGE_DB)
    p.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS)
    args = p.parse_args()

    backend_root = Path(args.backend_root).resolve()
    stage_db = (backend_root / args.stage_db).resolve()
    reports_dir = (backend_root / args.reports_dir / now_ts()).resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    print("========================================================================")
    print("VERIFY PDF EXAMPLE STAGE HARVEST")
    print("========================================================================")
    print(f"backend_root: {backend_root}")
    print(f"stage_db:     {stage_db}")
    print(f"report_dir:   {reports_dir}")

    con = sqlite3.connect(stage_db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    try:
        latest_page_run = fetch_scalar(cur, "select report_run from pdf_example_pages order by id desc limit 1")
        latest_region_run = fetch_scalar(cur, "select report_run from pdf_example_regions order by id desc limit 1")
        latest_extract_run = fetch_scalar(cur, "select report_run from pdf_example_extractions order by id desc limit 1")

        summary = {
            "stage_db": str(stage_db),
            "latest_page_run": latest_page_run,
            "latest_region_run": latest_region_run,
            "latest_extract_run": latest_extract_run,
            "totals": {
                "page_rows": fetch_scalar(cur, "select count(*) from pdf_example_pages"),
                "region_rows": fetch_scalar(cur, "select count(*) from pdf_example_regions"),
                "extraction_rows": fetch_scalar(cur, "select count(*) from pdf_example_extractions"),
            },
            "latest_stage_harvest": None,
            "latest_extraction": None,
        }

        if latest_region_run:
            fams = fetch_scalar(cur, "select count(distinct family_name_norm) from pdf_example_regions where report_run = ?", (latest_region_run,))
            pages = fetch_scalar(cur, "select count(*) from pdf_example_pages where report_run = ?", (latest_region_run,))
            regions = fetch_scalar(cur, "select count(*) from pdf_example_regions where report_run = ?", (latest_region_run,))
            avg_regions = fetch_scalar(cur, "select round(avg(c), 3) from (select count(*) as c from pdf_example_regions where report_run = ? group by family_name_norm)", (latest_region_run,))
            top_sparse = [dict(row) for row in cur.execute(
                """
                select family_name, count(*) as region_count
                from pdf_example_regions
                where report_run = ?
                group by family_name
                order by region_count asc, family_name asc
                limit 15
                """,
                (latest_region_run,),
            )]
            top_dense = [dict(row) for row in cur.execute(
                """
                select family_name, count(*) as region_count
                from pdf_example_regions
                where report_run = ?
                group by family_name
                order by region_count desc, family_name asc
                limit 15
                """,
                (latest_region_run,),
            )]
            counter = Counter()
            for row in cur.execute(
                "select family_name, count(*) as region_count from pdf_example_regions where report_run = ? group by family_name",
                (latest_region_run,),
            ):
                counter[int(row["region_count"])] += 1
            summary["latest_stage_harvest"] = {
                "report_run": latest_region_run,
                "families": fams,
                "pages": pages,
                "regions": regions,
                "avg_regions_per_family": avg_regions,
                "region_count_histogram": dict(sorted(counter.items())),
                "sparse_examples": top_sparse,
                "dense_examples": top_dense,
            }
            print(f"[HARVEST] run={latest_region_run} families={fams} pages={pages} regions={regions} avg_regions_per_family={avg_regions}")

        if latest_extract_run:
            rows = [dict(row) for row in cur.execute(
                """
                select status, count(*) as n
                from pdf_example_extractions
                where report_run = ?
                group by status
                order by status
                """,
                (latest_extract_run,),
            )]
            status_counts = {r["status"]: r["n"] for r in rows}
            ok_count = status_counts.get("ok", 0)
            nonnull_target = fetch_scalar(cur, "select count(*) from pdf_example_extractions where report_run = ? and coalesce(example_target_name,'') <> ''", (latest_extract_run,))
            raw_ok = [json.loads(r[0]) for r in cur.execute(
                "select raw_json from pdf_example_extractions where report_run = ? and status = 'ok' and raw_json is not null",
                (latest_extract_run,),
            )]
            reactant_nonnull = 0
            product_nonnull = 0
            both_nonnull = 0
            for obj in raw_ok:
                for ex in obj.get("extracts", []):
                    r = ex.get("reactant_smiles")
                    p = ex.get("product_smiles")
                    if r:
                        reactant_nonnull += 1
                    if p:
                        product_nonnull += 1
                    if r and p:
                        both_nonnull += 1
            summary["latest_extraction"] = {
                "report_run": latest_extract_run,
                "status_counts": status_counts,
                "target_name_nonnull": nonnull_target,
                "reactant_smiles_nonnull": reactant_nonnull,
                "product_smiles_nonnull": product_nonnull,
                "both_smiles_nonnull": both_nonnull,
            }
            print(f"[EXTRACT] run={latest_extract_run} status_counts={status_counts} target_name_nonnull={nonnull_target} reactant_smiles_nonnull={reactant_nonnull} product_smiles_nonnull={product_nonnull} both_smiles_nonnull={both_nonnull}")

        summary_json = reports_dir / "pdf_example_stage_verification_summary.json"
        summary_md = reports_dir / "pdf_example_stage_verification_summary.md"
        summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        md_lines = [
            "# PDF Example Stage Verification",
            "",
            f"- stage_db: `{stage_db}`",
            f"- latest_page_run: `{latest_page_run}`",
            f"- latest_region_run: `{latest_region_run}`",
            f"- latest_extract_run: `{latest_extract_run}`",
            "",
            "## Totals",
            "",
            f"- page_rows: **{summary['totals']['page_rows']}**",
            f"- region_rows: **{summary['totals']['region_rows']}**",
            f"- extraction_rows: **{summary['totals']['extraction_rows']}**",
            "",
        ]
        if summary["latest_stage_harvest"]:
            s = summary["latest_stage_harvest"]
            md_lines += [
                "## Latest Stage Harvest",
                "",
                f"- report_run: `{s['report_run']}`",
                f"- families: **{s['families']}**",
                f"- pages: **{s['pages']}**",
                f"- regions: **{s['regions']}**",
                f"- avg_regions_per_family: **{s['avg_regions_per_family']}**",
                f"- region_count_histogram: `{s['region_count_histogram']}`",
                "",
            ]
        if summary["latest_extraction"]:
            s = summary["latest_extraction"]
            md_lines += [
                "## Latest Extraction Run",
                "",
                f"- report_run: `{s['report_run']}`",
                f"- status_counts: `{s['status_counts']}`",
                f"- target_name_nonnull: **{s['target_name_nonnull']}**",
                f"- reactant_smiles_nonnull: **{s['reactant_smiles_nonnull']}**",
                f"- product_smiles_nonnull: **{s['product_smiles_nonnull']}**",
                f"- both_smiles_nonnull: **{s['both_smiles_nonnull']}**",
                "",
            ]
        summary_md.write_text("\n".join(md_lines), encoding="utf-8")
        print(f"summary json: {summary_json}")
        print(f"summary md:   {summary_md}")
        print("========================================================================")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
