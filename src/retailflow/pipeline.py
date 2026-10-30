from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
import uuid
from retailflow.config import RetailFlowConfig, get_config
from retailflow.db import Engine, create_db_engine, initialize_warehouse, text
from retailflow.ingestion import ingest_csv_to_raw
from retailflow.quality import (
    QualityCheckRecord,
    run_staging_quality_checks,
    run_warehouse_quality_checks,
)
from retailflow.staging import transform_raw_to_staging
from retailflow.warehouse import build_warehouse_star_schema


@dataclass(frozen=True)
class PipelineRunSummary:
    run_id: str
    batch_id: str
    status: str
    source_file: str
    raw_row_count: int
    valid_row_count: int
    quarantined_row_count: int
    duplicate_row_count: int
    warehouse_fact_upserts: int
    quality_checks_total: int
    quality_checks_passed: int
    quality_checks_failed: int
    duration_seconds: float
    error_stage: str | None = None
    error_message: str | None = None


def generate_run_id(batch_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid.uuid4().hex[:6]
    return f"RUN_{batch_id}_{ts}_{short_uuid}"


def start_pipeline_run_record(
    run_id: str,
    batch_id: str,
    source_file: str,
    engine: Engine,
    dag_id: str = "retailflow_daily_warehouse_etl",
    execution_mode: str = "local_orchestrator",
) -> str:
    started_at = datetime.now(timezone.utc).isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ops_pipeline_runs (
                    run_id, batch_id, dag_id, execution_mode, status,
                    started_at, source_file
                ) VALUES (
                    :run_id, :batch_id, :dag_id, :execution_mode, 'RUNNING',
                    :started_at, :source_file
                )
                """
            ),
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "dag_id": dag_id,
                "execution_mode": execution_mode,
                "started_at": started_at,
                "source_file": source_file,
            },
        )
    return started_at


def finalize_pipeline_run_record(
    run_id: str,
    status: str,
    duration_seconds: float,
    raw_row_count: int,
    valid_row_count: int,
    quarantined_row_count: int,
    duplicate_row_count: int,
    warehouse_fact_upserts: int,
    quality_checks_total: int,
    quality_checks_passed: int,
    quality_checks_failed: int,
    engine: Engine,
    error_stage: str | None = None,
    error_message: str | None = None,
) -> None:
    finished_at = datetime.now(timezone.utc).isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE ops_pipeline_runs
                SET status = :status,
                    finished_at = :finished_at,
                    duration_seconds = :duration_seconds,
                    raw_row_count = :raw_row_count,
                    valid_row_count = :valid_row_count,
                    quarantined_row_count = :quarantined_row_count,
                    duplicate_row_count = :duplicate_row_count,
                    warehouse_fact_upserts = :warehouse_fact_upserts,
                    quality_checks_total = :quality_checks_total,
                    quality_checks_passed = :quality_checks_passed,
                    quality_checks_failed = :quality_checks_failed,
                    error_stage = :error_stage,
                    error_message = :error_message
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "finished_at": finished_at,
                "duration_seconds": round(duration_seconds, 4),
                "raw_row_count": raw_row_count,
                "valid_row_count": valid_row_count,
                "quarantined_row_count": quarantined_row_count,
                "duplicate_row_count": duplicate_row_count,
                "warehouse_fact_upserts": warehouse_fact_upserts,
                "quality_checks_total": quality_checks_total,
                "quality_checks_passed": quality_checks_passed,
                "quality_checks_failed": quality_checks_failed,
                "error_stage": error_stage,
                "error_message": error_message,
            },
        )


def execute_retailflow_pipeline(
    csv_path: Path,
    batch_id: str,
    engine: Engine | None = None,
    config: RetailFlowConfig | None = None,
    dag_id: str = "retailflow_daily_warehouse_etl",
    execution_mode: str = "local_orchestrator",
    raise_on_failure: bool = True,
) -> PipelineRunSummary:
    cfg = config or get_config()
    eng = engine or create_db_engine(cfg.db_url)
    initialize_warehouse(eng, cfg)

    run_id = generate_run_id(batch_id)
    start_ts = time.perf_counter()
    start_pipeline_run_record(
        run_id=run_id,
        batch_id=batch_id,
        source_file=csv_path.name,
        engine=eng,
        dag_id=dag_id,
        execution_mode=execution_mode,
    )

    raw_count = 0
    valid_count = 0
    quarantined_count = 0
    duplicate_count = 0
    fact_upserts = 0
    current_stage = "INGEST_RAW"

    try:
        raw_count = ingest_csv_to_raw(
            csv_path=csv_path,
            batch_id=batch_id,
            run_id=run_id,
            engine=eng,
        )

        current_stage = "TRANSFORM_STAGING"
        stg_res = transform_raw_to_staging(
            batch_id=batch_id,
            run_id=run_id,
            engine=eng,
            config=cfg,
        )
        valid_count = stg_res.valid_count
        quarantined_count = stg_res.quarantined_count
        duplicate_count = stg_res.duplicate_count

        current_stage = "QUALITY_GATE_STAGING"
        run_staging_quality_checks(
            batch_id=batch_id,
            run_id=run_id,
            engine=eng,
            config=cfg,
        )

        current_stage = "LOAD_WAREHOUSE_STAR_SCHEMA"
        wh_res = build_warehouse_star_schema(
            batch_id=batch_id,
            run_id=run_id,
            engine=eng,
        )
        fact_upserts = wh_res.facts_upserted

        current_stage = "QUALITY_GATE_WAREHOUSE"
        run_warehouse_quality_checks(
            batch_id=batch_id,
            run_id=run_id,
            engine=eng,
        )

        duration = time.perf_counter() - start_ts
        with eng.connect() as conn:
            q_total = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM ops_quality_checks WHERE run_id = :r"),
                    {"r": run_id},
                ).scalar()
                or 0
            )
            q_passed = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM ops_quality_checks WHERE run_id = :r AND passed = 1"),
                    {"r": run_id},
                ).scalar()
                or 0
            )
        q_failed = q_total - q_passed

        finalize_pipeline_run_record(
            run_id=run_id,
            status="SUCCESS",
            duration_seconds=duration,
            raw_row_count=raw_count,
            valid_row_count=valid_count,
            quarantined_row_count=quarantined_count,
            duplicate_row_count=duplicate_count,
            warehouse_fact_upserts=fact_upserts,
            quality_checks_total=q_total,
            quality_checks_passed=q_passed,
            quality_checks_failed=q_failed,
            engine=eng,
        )

        return PipelineRunSummary(
            run_id=run_id,
            batch_id=batch_id,
            status="SUCCESS",
            source_file=csv_path.name,
            raw_row_count=raw_count,
            valid_row_count=valid_count,
            quarantined_row_count=quarantined_count,
            duplicate_row_count=duplicate_count,
            warehouse_fact_upserts=fact_upserts,
            quality_checks_total=q_total,
            quality_checks_passed=q_passed,
            quality_checks_failed=q_failed,
            duration_seconds=round(duration, 4),
        )

    except Exception as exc:
        duration = time.perf_counter() - start_ts
        with eng.connect() as conn:
            q_total = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM ops_quality_checks WHERE run_id = :r"),
                    {"r": run_id},
                ).scalar()
                or 0
            )
            q_passed = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM ops_quality_checks WHERE run_id = :r AND passed = 1"),
                    {"r": run_id},
                ).scalar()
                or 0
            )
        q_failed = q_total - q_passed

        finalize_pipeline_run_record(
            run_id=run_id,
            status="FAILED",
            duration_seconds=duration,
            raw_row_count=raw_count,
            valid_row_count=valid_count,
            quarantined_row_count=quarantined_count,
            duplicate_row_count=duplicate_count,
            warehouse_fact_upserts=fact_upserts,
            quality_checks_total=q_total,
            quality_checks_passed=q_passed,
            quality_checks_failed=q_failed,
            engine=eng,
            error_stage=current_stage,
            error_message=str(exc),
        )

        if raise_on_failure:
            raise

        return PipelineRunSummary(
            run_id=run_id,
            batch_id=batch_id,
            status="FAILED",
            source_file=csv_path.name,
            raw_row_count=raw_count,
            valid_row_count=valid_count,
            quarantined_row_count=quarantined_count,
            duplicate_row_count=duplicate_count,
            warehouse_fact_upserts=fact_upserts,
            quality_checks_total=q_total,
            quality_checks_passed=q_passed,
            quality_checks_failed=q_failed,
            duration_seconds=round(duration, 4),
            error_stage=current_stage,
            error_message=str(exc),
        )
