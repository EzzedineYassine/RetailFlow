from pathlib import Path
import pandas as pd
import pytest

from retailflow.analytics import (
    ANALYTICS_QUERIES,
    execute_named_report,
    get_executive_kpi_summary,
    get_pipeline_runs_df,
    get_quality_checks_df,
    get_quarantine_records_df,
)
from retailflow.config import GUEST_CUSTOMER_ID, RetailFlowConfig
from retailflow.db import create_db_engine, initialize_warehouse, text
from retailflow.ingestion import SchemaValidationError, ingest_csv_to_raw
from retailflow.pipeline import execute_retailflow_pipeline
from retailflow.quality import DataQualityGateError
from retailflow.sample_data import generate_synthetic_retail_batch


@pytest.fixture()
def isolated_env(tmp_path: Path):
    project_root = Path(__file__).resolve().parents[1]
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    wh_dir = data_dir / "warehouse"
    raw_dir.mkdir(parents=True, exist_ok=True)
    wh_dir.mkdir(parents=True, exist_ok=True)

    db_file = wh_dir / "test_retailflow.db"
    db_url = f"sqlite:///{db_file.as_posix()}"

    cfg = RetailFlowConfig(
        env="test",
        db_url=db_url,
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        warehouse_dir=wh_dir,
        sql_dir=project_root / "sql",
        max_quarantine_rate=0.25,
    )
    engine = create_db_engine(cfg.db_url)
    initialize_warehouse(engine, cfg)
    return cfg, engine


def test_schema_validation_rejects_missing_columns(isolated_env, tmp_path: Path):
    cfg, engine = isolated_env
    bad_csv = tmp_path / "invalid_schema.csv"
    bad_csv.write_text("InvoiceNo,StockCode,Quantity\n5001,85123A,3\n", encoding="utf-8")

    with pytest.raises(SchemaValidationError) as exc_info:
        ingest_csv_to_raw(
            csv_path=bad_csv,
            batch_id="bad_schema_batch",
            run_id="RUN_TEST_SCHEMA",
            engine=engine,
        )
    assert "Missing required columns" in str(exc_info.value)


def test_cleaning_rules_and_quarantine_separation(isolated_env):
    cfg, engine = isolated_env
    csv_path = cfg.raw_dir / "edge_cases_batch.csv"

    rows = [
        "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country",
        "580001,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2026-09-10 09:30:00,2.95,12346,United Kingdom",
        "580001,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2026-09-10 09:30:00,2.95,12346,United Kingdom",
        "C580002,22423,REGENCY CAKESTAND 3 TIER,-2,2026-09-10 10:15:00,12.75,12347,Germany",
        "580003,85099B,JUMBO BAG RED RETROSPOT,10,2026-09-10 11:00:00,2.08,,United Kingdom",
        "580004,47566,PARTY BUNTING,4,2026-09-10 12:00:00,4.95,12348,France",
        "580005,84879,ASSORTED COLOUR BIRD ORNAMENT,8,2026-09-10 13:00:00,1.69,12349,United Kingdom",
        "580006,20725,LUNCH BAG RED RETROSPOT,5,2026-09-10 14:00:00,1.65,12350,Spain",
        "580007,22720,SET OF 3 CAKE TINS,3,2026-09-10 15:00:00,4.95,12351,United Kingdom",
        "580008,POST,POSTAGE,1,2026-09-10 16:00:00,18.00,12347,Germany",
        "580009,22197,POPCORN HOLDER,0,2026-09-10 16:30:00,0.85,12352,United Kingdom",
    ]
    csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    summary = execute_retailflow_pipeline(
        csv_path=csv_path,
        batch_id="edge_cases_batch",
        engine=engine,
        config=cfg,
    )

    assert summary.status == "SUCCESS"
    assert summary.raw_row_count == 10
    assert summary.duplicate_row_count == 1
    assert summary.quarantined_row_count == 2
    assert summary.valid_row_count == 7
    assert summary.warehouse_fact_upserts == 7

    quar_df = get_quarantine_records_df(engine, batch_id="edge_cases_batch")
    codes = set(quar_df["rejection_code"].tolist())
    assert "NON_MERCHANDISE_CODE" in codes
    assert "ZERO_QUANTITY" in codes

    with engine.connect() as conn:
        guest_row = conn.execute(
            text("SELECT customer_id, is_guest FROM dim_customer WHERE customer_id = :cid"),
            {"cid": GUEST_CUSTOMER_ID},
        ).mappings().first()
        assert guest_row is not None
        assert int(guest_row["is_guest"]) == 1

        cancel_row = conn.execute(
            text("SELECT transaction_type, is_cancelled, gross_amount, return_amount, net_amount FROM fact_sales WHERE invoice_no = 'C580002'"),
        ).mappings().first()
        assert cancel_row is not None
        assert cancel_row["transaction_type"] == "CANCELLATION"
        assert int(cancel_row["is_cancelled"]) == 1
        assert float(cancel_row["gross_amount"]) == 0.0
        assert float(cancel_row["return_amount"]) == 25.50
        assert float(cancel_row["net_amount"]) == -25.50


def test_idempotent_rerun_does_not_duplicate_warehouse_facts(isolated_env):
    cfg, engine = isolated_env
    csv_path = cfg.raw_dir / "idempotent_batch.csv"
    generate_synthetic_retail_batch(
        output_path=csv_path,
        batch_name="idempotent_batch",
        start_date="2026-08-01",
        days=20,
        num_invoices=60,
        seed=99,
    )

    first_run = execute_retailflow_pipeline(
        csv_path=csv_path,
        batch_id="idempotent_batch",
        engine=engine,
        config=cfg,
    )
    kpi_after_first = get_executive_kpi_summary(engine)

    second_run = execute_retailflow_pipeline(
        csv_path=csv_path,
        batch_id="idempotent_batch",
        engine=engine,
        config=cfg,
    )
    kpi_after_second = get_executive_kpi_summary(engine)

    assert first_run.status == "SUCCESS"
    assert second_run.status == "SUCCESS"
    assert kpi_after_first["total_fact_lines"] == kpi_after_second["total_fact_lines"]
    assert kpi_after_first["net_revenue"] == kpi_after_second["net_revenue"]
    assert kpi_after_first["completed_orders"] == kpi_after_second["completed_orders"]


def test_failed_quality_gate_protects_warehouse_and_records_failure(isolated_env):
    cfg, engine = isolated_env
    good_csv = cfg.raw_dir / "good_batch.csv"
    generate_synthetic_retail_batch(
        output_path=good_csv,
        batch_name="good_batch",
        start_date="2026-08-01",
        days=15,
        num_invoices=50,
        seed=12,
    )
    execute_retailflow_pipeline(
        csv_path=good_csv,
        batch_id="good_batch",
        engine=engine,
        config=cfg,
    )
    baseline_kpis = get_executive_kpi_summary(engine)

    corrupt_csv = cfg.raw_dir / "corrupt_batch.csv"
    corrupt_csv.write_text(
        "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
        "590001,85123A,HEART HOLDER,0,2026-09-01 10:00:00,2.95,12346,United Kingdom\n"
        "590002,22423,CAKESTAND,2,BAD_DATE,-5.00,12347,Germany\n"
        "590003,POST,POSTAGE,1,2026-09-01 11:00:00,15.00,12348,France\n"
        "590004,85099B,JUMBO BAG,2,2026-09-01 12:00:00,2.08,12349,United Kingdom\n",
        encoding="utf-8",
    )

    with pytest.raises(DataQualityGateError):
        execute_retailflow_pipeline(
            csv_path=corrupt_csv,
            batch_id="corrupt_batch",
            engine=engine,
            config=cfg,
            raise_on_failure=True,
        )

    post_fail_kpis = get_executive_kpi_summary(engine)
    assert post_fail_kpis["total_fact_lines"] == baseline_kpis["total_fact_lines"]
    assert post_fail_kpis["net_revenue"] == baseline_kpis["net_revenue"]

    runs_df = get_pipeline_runs_df(engine)
    latest_run = runs_df.iloc[0]
    assert latest_run["batch_id"] == "corrupt_batch"
    assert latest_run["status"] == "FAILED"
    assert latest_run["error_stage"] == "QUALITY_GATE_STAGING"

    checks_df = get_quality_checks_df(engine, run_id=str(latest_run["run_id"]))
    failed_checks = checks_df[checks_df["passed"] == 0]
    assert not failed_checks.empty


def test_all_analytical_sql_reports_execute_cleanly(isolated_env):
    cfg, engine = isolated_env
    csv_path = cfg.raw_dir / "analytics_batch.csv"
    generate_synthetic_retail_batch(
        output_path=csv_path,
        batch_name="analytics_batch",
        start_date="2026-08-01",
        days=45,
        num_invoices=120,
        seed=55,
    )
    execute_retailflow_pipeline(
        csv_path=csv_path,
        batch_id="analytics_batch",
        engine=engine,
        config=cfg,
    )

    for report_name in ANALYTICS_QUERIES:
        df = execute_named_report(report_name, engine=engine, config=cfg)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty, f"Report {report_name} returned an empty DataFrame"


def test_airflow_dag_task_callables_execute_in_order(isolated_env, monkeypatch):
    cfg, engine = isolated_env
    monkeypatch.setenv("RETAILFLOW_DB_URL", cfg.db_url)

    from dags.retailflow_etl_dag import (
        task_ensure_source_batch,
        task_finalize_run_audit,
        task_ingest_raw,
        task_load_warehouse_star_schema,
        task_transform_staging,
        task_validate_staging_quality,
        task_validate_warehouse_quality,
    )

    csv_path = cfg.raw_dir / "dag_callable_batch.csv"
    generate_synthetic_retail_batch(
        output_path=csv_path,
        batch_name="dag_callable_batch",
        start_date="2026-10-01",
        days=15,
        num_invoices=55,
        seed=88,
    )

    meta = task_ensure_source_batch(params={"batch_id": "dag_callable_batch", "csv_path": str(csv_path)})
    ctx = {"run_meta": meta}

    raw_cnt = task_ingest_raw(**ctx)
    assert raw_cnt > 0

    stg_summary = task_transform_staging(**ctx)
    assert stg_summary["valid_count"] > 0

    stg_checks = task_validate_staging_quality(**ctx)
    assert stg_checks == 5

    fact_upserts = task_load_warehouse_star_schema(**ctx)
    assert fact_upserts == stg_summary["valid_count"]

    wh_checks = task_validate_warehouse_quality(**ctx)
    assert wh_checks == 3

    task_finalize_run_audit(**ctx)

    runs_df = get_pipeline_runs_df(engine)
    assert runs_df.iloc[0]["status"] == "SUCCESS"
    assert runs_df.iloc[0]["batch_id"] == "dag_callable_batch"


def test_workbench_fastapi_endpoints(isolated_env, monkeypatch):
    cfg, _ = isolated_env
    monkeypatch.setenv("RETAILFLOW_DB_URL", cfg.db_url)

    from starlette.testclient import TestClient
    from retailflow.workbench_server import app

    client = TestClient(app)
    res_html = client.get("/")
    assert res_html.status_code == 200
    assert "RETAILFLOW" in res_html.text

    res_ov = client.get("/api/overview")
    assert res_ov.status_code == 200
    payload = res_ov.json()
    assert payload["kpis"]["total_fact_lines"] > 0
    assert len(payload["runs"]) >= 2

    res_rep = client.get("/api/reports/best_selling_products")
    assert res_rep.status_code == 200
    assert len(res_rep.json()["rows"]) > 0

    res_corrupt = client.post("/api/trigger/corrupt", json={"batch_id": "api_corrupt_test"})
    assert res_corrupt.status_code == 200
    assert res_corrupt.json()["status"] == "FAILED"

