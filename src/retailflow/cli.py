import argparse
import json
from pathlib import Path
import sys
from retailflow.analytics import (
    ANALYTICS_QUERIES,
    execute_named_report,
    get_executive_kpi_summary,
    get_pipeline_runs_df,
)
from retailflow.config import get_config
from retailflow.db import create_db_engine, initialize_warehouse
from retailflow.pipeline import execute_retailflow_pipeline
from retailflow.sample_data import generate_synthetic_retail_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="retailflow",
        description="RetailFlow Data Engineering & Warehouse Orchestration CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize raw, staging, warehouse, and ops tables")

    gen_p = sub.add_parser("generate-sample", help="Generate deterministic synthetic UCI-schema retail CSV batch")
    gen_p.add_argument("--batch-id", default="2026_10_batch_01", help="Batch identifier")
    gen_p.add_argument("--start-date", default="2026-08-01", help="Start date YYYY-MM-DD")
    gen_p.add_argument("--days", type=int, default=90, help="Number of days to span")
    gen_p.add_argument("--invoices", type=int, default=420, help="Number of invoices to generate")
    gen_p.add_argument("--seed", type=int, default=42, help="Random seed")

    run_p = sub.add_parser("run-pipeline", help="Execute full end-to-end ETL + DQ checks for a CSV batch")
    run_p.add_argument("--csv", required=True, help="Path to input CSV file")
    run_p.add_argument("--batch-id", required=True, help="Batch identifier")
    run_p.add_argument("--mode", default="cli_manual", help="Execution mode label")

    seed_p = sub.add_parser("seed-demo", help="Initialize DB, generate two multi-month batches, and run ETL")
    seed_p.add_argument("--include-failed-run-demo", action="store_true", help="Also simulate a quarantined/failed batch for observability")

    rep_p = sub.add_parser("report", help="Run a version-controlled SQL analytical report")
    rep_p.add_argument("--name", choices=list(ANALYTICS_QUERIES.keys()), default="revenue_trends")
    rep_p.add_argument("--limit", type=int, default=15)

    sub.add_parser("status", help="Show executive KPIs and recent pipeline runs")

    serve_p = sub.add_parser("serve-workbench", help="Start the FastAPI Operations & BI Workbench server")
    serve_p.add_argument("--host", default="127.0.0.1", help="Host interface")
    serve_p.add_argument("--port", type=int, default=8501, help="Server port")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)

    if args.command == "init-db":
        initialize_warehouse(engine, cfg)
        print(f"Warehouse initialized at: {cfg.db_url}")
        return 0

    if args.command == "generate-sample":
        out_path = cfg.raw_dir / f"{args.batch_id}.csv"
        generate_synthetic_retail_batch(
            output_path=out_path,
            batch_name=args.batch_id,
            start_date=args.start_date,
            days=args.days,
            num_invoices=args.invoices,
            seed=args.seed,
        )
        print(f"Generated synthetic retail batch: {out_path}")
        return 0

    if args.command == "run-pipeline":
        csv_path = Path(args.csv).resolve()
        summary = execute_retailflow_pipeline(
            csv_path=csv_path,
            batch_id=args.batch_id,
            engine=engine,
            config=cfg,
            execution_mode=args.mode,
            raise_on_failure=False,
        )
        print(json.dumps(summary.__dict__, indent=2))
        return 0 if summary.status == "SUCCESS" else 1

    if args.command == "seed-demo":
        initialize_warehouse(engine, cfg)
        b1_path = cfg.raw_dir / "2026_q3_uk_eu_retail.csv"
        b2_path = cfg.raw_dir / "2026_q4_oct_retail.csv"

        generate_synthetic_retail_batch(
            output_path=b1_path,
            batch_name="2026_q3_uk_eu_retail",
            start_date="2026-07-01",
            days=62,
            num_invoices=380,
            seed=42,
        )
        generate_synthetic_retail_batch(
            output_path=b2_path,
            batch_name="2026_q4_oct_retail",
            start_date="2026-09-01",
            days=58,
            num_invoices=340,
            seed=108,
        )

        s1 = execute_retailflow_pipeline(
            csv_path=b1_path,
            batch_id="2026_q3_uk_eu_retail",
            engine=engine,
            config=cfg,
            execution_mode="airflow_scheduled",
        )
        s2 = execute_retailflow_pipeline(
            csv_path=b2_path,
            batch_id="2026_q4_oct_retail",
            engine=engine,
            config=cfg,
            execution_mode="airflow_scheduled",
        )
        print(f"Batch 1 ({s1.batch_id}): {s1.status} | {s1.valid_row_count} valid rows | {s1.quarantined_row_count} quarantined")
        print(f"Batch 2 ({s2.batch_id}): {s2.status} | {s2.valid_row_count} valid rows | {s2.quarantined_row_count} quarantined")

        if args.include_failed_run_demo:
            corrupt_path = cfg.raw_dir / "2026_q4_corrupt_feed.csv"
            corrupt_path.write_text(
                "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
                "599001,85123A,WHITE HANGING HEART,0,2026-10-15 10:00:00,2.95,12346,United Kingdom\n"
                "599002,22423,REGENCY CAKESTAND,2,INVALID_DATE,-12.75,12347,United Kingdom\n"
                "599003,POST,POSTAGE,1,2026-10-15 11:00:00,15.00,12348,Germany\n"
                "599004,85099B,JUMBO BAG RED RETROSPOT,5,2026-10-15 12:00:00,2.08,12349,France\n",
                encoding="utf-8",
            )
            s_fail = execute_retailflow_pipeline(
                csv_path=corrupt_path,
                batch_id="2026_q4_corrupt_feed",
                engine=engine,
                config=cfg,
                execution_mode="airflow_scheduled",
                raise_on_failure=False,
            )
            print(f"Simulated Corrupt Batch ({s_fail.batch_id}): {s_fail.status} at {s_fail.error_stage} -> {s_fail.error_message}")

        return 0

    if args.command == "report":
        df = execute_named_report(args.name, engine=engine, config=cfg)
        print(df.head(args.limit).to_string(index=False))
        return 0

    if args.command == "status":
        kpis = get_executive_kpi_summary(engine)
        runs = get_pipeline_runs_df(engine, limit=10)
        print("=== EXECUTIVE WAREHOUSE SUMMARY ===")
        print(json.dumps(kpis, indent=2))
        print("\n=== RECENT PIPELINE RUNS ===")
        if runs.empty:
            print("No pipeline runs recorded.")
        else:
            cols = ["run_id", "batch_id", "status", "raw_row_count", "valid_row_count", "quarantined_row_count", "quality_checks_failed"]
            print(runs[cols].to_string(index=False))
        return 0

    if args.command == "serve-workbench":
        import uvicorn
        from retailflow.workbench_server import app
        uvicorn.run(app, host=args.host, port=args.port)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
