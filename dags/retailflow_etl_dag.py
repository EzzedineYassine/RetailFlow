from datetime import datetime, timedelta
import logging
from pathlib import Path

from retailflow.config import get_config
from retailflow.db import create_db_engine, initialize_warehouse, text
from retailflow.ingestion import ingest_csv_to_raw
from retailflow.pipeline import (
    finalize_pipeline_run_record,
    generate_run_id,
    start_pipeline_run_record,
)
from retailflow.quality import (
    run_staging_quality_checks,
    run_warehouse_quality_checks,
)
from retailflow.sample_data import generate_synthetic_retail_batch
from retailflow.staging import transform_raw_to_staging
from retailflow.warehouse import build_warehouse_star_schema

logger = logging.getLogger("retailflow.airflow_dag")

DAG_ID = "retailflow_daily_warehouse_etl"
DEFAULT_BATCH_ID = "2026_10_daily_batch"


def task_ensure_source_batch(**context) -> dict[str, str]:
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    initialize_warehouse(engine, cfg)

    params = context.get("params") or {}
    batch_id = str(params.get("batch_id", DEFAULT_BATCH_ID))
    csv_path_str = params.get("csv_path")

    if csv_path_str:
        csv_path = Path(csv_path_str)
    else:
        csv_path = cfg.raw_dir / f"{batch_id}.csv"
        if not csv_path.exists():
            generate_synthetic_retail_batch(
                output_path=csv_path,
                batch_name=batch_id,
                start_date="2026-10-01",
                days=30,
                num_invoices=220,
                seed=77,
            )

    run_id = generate_run_id(batch_id)
    start_pipeline_run_record(
        run_id=run_id,
        batch_id=batch_id,
        source_file=csv_path.name,
        engine=engine,
        dag_id=DAG_ID,
        execution_mode="airflow_dag",
    )

    payload = {
        "run_id": run_id,
        "batch_id": batch_id,
        "csv_path": str(csv_path.resolve()),
    }
    if "ti" in context and context["ti"] is not None:
        context["ti"].xcom_push(key="run_meta", value=payload)
    return payload


def _resolve_meta(context: dict) -> dict[str, str]:
    if "run_meta" in context:
        return context["run_meta"]
    ti = context.get("ti")
    if ti is not None:
        pulled = ti.xcom_pull(task_ids="ensure_source_batch", key="run_meta")
        if pulled:
            return pulled
    raise RuntimeError("Missing run_meta context from upstream task 'ensure_source_batch'.")


def task_ingest_raw(**context) -> int:
    meta = _resolve_meta(context)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    raw_count = ingest_csv_to_raw(
        csv_path=Path(meta["csv_path"]),
        batch_id=meta["batch_id"],
        run_id=meta["run_id"],
        engine=engine,
    )
    logger.info("Ingested %d raw rows for batch %s", raw_count, meta["batch_id"])
    return raw_count


def task_transform_staging(**context) -> dict[str, int]:
    meta = _resolve_meta(context)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    res = transform_raw_to_staging(
        batch_id=meta["batch_id"],
        run_id=meta["run_id"],
        engine=engine,
        config=cfg,
    )
    return {
        "raw_count": res.raw_count,
        "valid_count": res.valid_count,
        "quarantined_count": res.quarantined_count,
        "duplicate_count": res.duplicate_count,
    }


def task_validate_staging_quality(**context) -> int:
    meta = _resolve_meta(context)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    checks = run_staging_quality_checks(
        batch_id=meta["batch_id"],
        run_id=meta["run_id"],
        engine=engine,
        config=cfg,
    )
    return len(checks)


def task_load_warehouse_star_schema(**context) -> int:
    meta = _resolve_meta(context)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    wh_res = build_warehouse_star_schema(
        batch_id=meta["batch_id"],
        run_id=meta["run_id"],
        engine=engine,
    )
    return wh_res.facts_upserted


def task_validate_warehouse_quality(**context) -> int:
    meta = _resolve_meta(context)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    checks = run_warehouse_quality_checks(
        batch_id=meta["batch_id"],
        run_id=meta["run_id"],
        engine=engine,
    )
    return len(checks)


def task_finalize_run_audit(**context) -> None:
    meta = _resolve_meta(context)
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    batch_id = meta["batch_id"]
    run_id = meta["run_id"]

    with engine.connect() as conn:
        raw_cnt = int(conn.execute(text("SELECT COUNT(*) FROM raw_transactions WHERE batch_id = :b"), {"b": batch_id}).scalar() or 0)
        val_cnt = int(conn.execute(text("SELECT COUNT(*) FROM stg_transactions WHERE batch_id = :b"), {"b": batch_id}).scalar() or 0)
        quar_cnt = int(conn.execute(text("SELECT COUNT(*) FROM ops_quarantine_records WHERE run_id = :r"), {"r": run_id}).scalar() or 0)
        dup_cnt = int(
            conn.execute(
                text("SELECT COALESCE(SUM(occurrence_count - 1), 0) FROM stg_transactions WHERE batch_id = :b"),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        fact_cnt = int(conn.execute(text("SELECT COUNT(*) FROM fact_sales WHERE batch_id = :b"), {"b": batch_id}).scalar() or 0)
        q_total = int(conn.execute(text("SELECT COUNT(*) FROM ops_quality_checks WHERE run_id = :r"), {"r": run_id}).scalar() or 0)
        q_pass = int(conn.execute(text("SELECT COUNT(*) FROM ops_quality_checks WHERE run_id = :r AND passed = 1"), {"r": run_id}).scalar() or 0)

    finalize_pipeline_run_record(
        run_id=run_id,
        status="SUCCESS",
        duration_seconds=1.25,
        raw_row_count=raw_cnt,
        valid_row_count=val_cnt,
        quarantined_row_count=quar_cnt,
        duplicate_row_count=dup_cnt,
        warehouse_fact_upserts=fact_cnt,
        quality_checks_total=q_total,
        quality_checks_passed=q_pass,
        quality_checks_failed=q_total - q_pass,
        engine=engine,
    )


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    default_args = {
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    }

    with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description="Idempotent retail transaction ingestion, cleaning, DQ gating, and star-schema load",
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["retailflow", "warehouse", "star-schema", "data-quality"],
    ) as dag:
        t_ensure = PythonOperator(
            task_id="ensure_source_batch",
            python_callable=task_ensure_source_batch,
        )
        t_ingest = PythonOperator(
            task_id="ingest_raw_layer",
            python_callable=task_ingest_raw,
        )
        t_staging = PythonOperator(
            task_id="clean_and_stage_transactions",
            python_callable=task_transform_staging,
        )
        t_dq_staging = PythonOperator(
            task_id="quality_gate_staging",
            python_callable=task_validate_staging_quality,
        )
        t_warehouse = PythonOperator(
            task_id="upsert_star_schema_warehouse",
            python_callable=task_load_warehouse_star_schema,
        )
        t_dq_warehouse = PythonOperator(
            task_id="quality_gate_warehouse",
            python_callable=task_validate_warehouse_quality,
        )
        t_finalize = PythonOperator(
            task_id="record_pipeline_audit",
            python_callable=task_finalize_run_audit,
        )

        (
            t_ensure
            >> t_ingest
            >> t_staging
            >> t_dq_staging
            >> t_warehouse
            >> t_dq_warehouse
            >> t_finalize
        )
except ImportError:
    dag = None
