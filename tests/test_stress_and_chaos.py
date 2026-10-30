import pytest
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from retailflow.config import RetailFlowConfig
from retailflow.db import create_db_engine, initialize_warehouse, text
from retailflow.ingestion import ingest_csv_to_raw, SchemaValidationError
from retailflow.staging import transform_raw_to_staging
from retailflow.quality import run_staging_quality_checks, run_warehouse_quality_checks, DataQualityGateError
from retailflow.pipeline import execute_retailflow_pipeline
from retailflow.sample_data import generate_synthetic_retail_batch
from retailflow.analytics import execute_named_report, get_executive_kpi_summary, get_pipeline_runs_df


@pytest.fixture()
def stress_env(tmp_path: Path):
    project_root = Path(__file__).resolve().parents[1]
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    wh_dir = data_dir / "warehouse"
    raw_dir.mkdir(parents=True, exist_ok=True)
    wh_dir.mkdir(parents=True, exist_ok=True)

    db_file = wh_dir / "stress_retailflow.db"
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


def test_extreme_chaos_and_dirty_data_rejection(stress_env, tmp_path: Path):
    cfg, engine = stress_env
    chaos_csv = tmp_path / "extreme_chaos_feed.csv"

    rows = [
        "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country",
        ",85123A,NULL INVOICE NUMBER,5,2026-10-15 10:00:00,2.95,12345,United Kingdom",
        "599101,,MISSING STOCK CODE,2,2026-10-15 10:05:00,4.50,12346,United Kingdom",
        "599102,POST,POSTAGE EXPENSE,1,2026-10-15 10:10:00,18.00,12347,Germany",
        "599103,BANK CHARGES,SERVICE CHARGE,1,2026-10-15 10:15:00,15.00,12348,United Kingdom",
        "599104,85099B,ZERO QUANTITY LINE,0,2026-10-15 10:20:00,2.08,12349,France",
        "599105,22423,OUTLIER QUANTITY,99999999,2026-10-15 10:25:00,12.75,12350,United Kingdom",
        "599106,47566,MASSIVE NEGATIVE PRICE,4,2026-10-15 10:30:00,-9999.00,12351,United Kingdom",
        "599107,84879,ZERO PRICE LINE,3,2026-10-15 10:35:00,0.00,12352,United Kingdom",
        "599108,20725,INVALID DATE STRING,5,CORRUPT_TIMESTAMP,1.65,12353,Spain",
        ",,,,,,,",
        "599109,22720,VALID TRANSACTION WITH ISO TZ,2,2026-10-15T11:00:00+02:00,4.95,12354,France",
    ]

    with open(chaos_csv, "wb") as f:
        content = "\n".join(rows) + "\n"
        f.write(content.encode("utf-8-sig"))

    raw_cnt = ingest_csv_to_raw(
        csv_path=chaos_csv,
        batch_id="chaos_batch",
        run_id="RUN_CHAOS_01",
        engine=engine,
    )
    assert raw_cnt == 11

    stg_res = transform_raw_to_staging(
        batch_id="chaos_batch",
        run_id="RUN_CHAOS_01",
        engine=engine,
        config=cfg,
    )

    assert stg_res.quarantined_count == 10
    assert stg_res.valid_count == 1

    with engine.connect() as conn:
        quar_codes = conn.to_dataframe(
            text("SELECT rejection_code, COUNT(*) as cnt FROM ops_quarantine_records WHERE batch_id = 'chaos_batch' GROUP BY rejection_code")
        )
        codes_set = set(quar_codes["rejection_code"].tolist())

    expected_codes = {
        "MISSING_INVOICE_NO",
        "MISSING_STOCK_CODE",
        "NON_MERCHANDISE_CODE",
        "ZERO_QUANTITY",
        "EXTREME_QUANTITY_OUTLIER",
        "NON_POSITIVE_UNIT_PRICE",
        "INVALID_INVOICE_DATE",
        "COMPLETELY_BLANK_ROW",
    }
    assert expected_codes.issubset(codes_set)

    with pytest.raises(DataQualityGateError) as exc_info:
        run_staging_quality_checks(
            batch_id="chaos_batch",
            run_id="RUN_CHAOS_01",
            engine=engine,
            config=cfg,
        )
    assert "quarantine_rate_within_bounds" in str(exc_info.value)

    with engine.connect() as conn:
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
    assert fact_count == 0


def test_non_utf8_encoding_resilience(stress_env, tmp_path: Path):
    cfg, engine = stress_env
    iso_csv = tmp_path / "latin1_corrupt_feed.csv"

    header = "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
    special_row = "599201,85123A,CAFE MÜLLER SPECIAL DÉCOR,3,2026-10-15 12:00:00,3.50,12360,Germany\n"

    with open(iso_csv, "wb") as f:
        f.write(header.encode("latin-1"))
        f.write(special_row.encode("latin-1"))

    raw_cnt = ingest_csv_to_raw(
        csv_path=iso_csv,
        batch_id="latin1_batch",
        run_id="RUN_LATIN1_01",
        engine=engine,
    )
    assert raw_cnt == 1

    stg_res = transform_raw_to_staging(
        batch_id="latin1_batch",
        run_id="RUN_LATIN1_01",
        engine=engine,
        config=cfg,
    )
    assert stg_res.valid_count == 1
    assert stg_res.quarantined_count == 0


def test_triple_consecutive_ingestion_idempotency(stress_env):
    cfg, engine = stress_env
    batch_path = cfg.raw_dir / "idempotency_stress_batch.csv"

    generate_synthetic_retail_batch(
        output_path=batch_path,
        batch_name="idempotency_stress_batch",
        start_date="2026-08-01",
        days=30,
        num_invoices=100,
        seed=333,
    )

    run_1 = execute_retailflow_pipeline(
        csv_path=batch_path,
        batch_id="idempotency_stress_batch",
        engine=engine,
        config=cfg,
    )
    kpis_1 = get_executive_kpi_summary(engine)

    run_2 = execute_retailflow_pipeline(
        csv_path=batch_path,
        batch_id="idempotency_stress_batch",
        engine=engine,
        config=cfg,
    )
    kpis_2 = get_executive_kpi_summary(engine)

    run_3 = execute_retailflow_pipeline(
        csv_path=batch_path,
        batch_id="idempotency_stress_batch",
        engine=engine,
        config=cfg,
    )
    kpis_3 = get_executive_kpi_summary(engine)

    assert run_1.status == "SUCCESS"
    assert run_2.status == "SUCCESS"
    assert run_3.status == "SUCCESS"

    assert kpis_1["total_fact_lines"] == kpis_2["total_fact_lines"] == kpis_3["total_fact_lines"]
    assert kpis_1["completed_orders"] == kpis_2["completed_orders"] == kpis_3["completed_orders"]
    assert kpis_1["cancelled_orders"] == kpis_2["cancelled_orders"] == kpis_3["cancelled_orders"]
    assert kpis_1["gross_revenue"] == kpis_2["gross_revenue"] == kpis_3["gross_revenue"]
    assert kpis_1["return_amount"] == kpis_2["return_amount"] == kpis_3["return_amount"]
    assert kpis_1["net_revenue"] == kpis_2["net_revenue"] == kpis_3["net_revenue"]

    with engine.connect() as conn:
        dup_fact_ids = conn.execute(
            text("SELECT sales_fact_id, COUNT(*) FROM fact_sales GROUP BY sales_fact_id HAVING COUNT(*) > 1")
        ).scalar()
        assert dup_fact_ids is None or dup_fact_ids == 0

        stg_occurrences = conn.to_dataframe(
            text("SELECT occurrence_count FROM stg_transactions WHERE occurrence_count > 1")
        )
        assert len(stg_occurrences) > 0


def test_mathematical_proofs_and_guest_isolation(stress_env):
    cfg, engine = stress_env
    b1_path = cfg.raw_dir / "math_proof_batch_1.csv"
    b2_path = cfg.raw_dir / "math_proof_batch_2.csv"

    generate_synthetic_retail_batch(
        output_path=b1_path,
        batch_name="math_proof_batch_1",
        start_date="2026-07-01",
        days=45,
        num_invoices=150,
        seed=101,
    )
    generate_synthetic_retail_batch(
        output_path=b2_path,
        batch_name="math_proof_batch_2",
        start_date="2026-08-15",
        days=45,
        num_invoices=150,
        seed=202,
    )

    execute_retailflow_pipeline(csv_path=b1_path, batch_id="math_proof_batch_1", engine=engine, config=cfg)
    execute_retailflow_pipeline(csv_path=b2_path, batch_id="math_proof_batch_2", engine=engine, config=cfg)

    with engine.connect() as conn:
        identity_violations = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_sales
                WHERE ABS((gross_amount - return_amount) - net_amount) > 0.0001
                """
            )
        ).scalar()
        assert identity_violations == 0

        cancel_violations = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_sales
                WHERE is_cancelled = 1 AND (gross_amount != 0.0 OR net_amount > 0.0 OR quantity >= 0)
                """
            )
        ).scalar()
        assert cancel_violations == 0

        guest_in_fact = conn.execute(
            text("SELECT COUNT(*) FROM fact_sales WHERE customer_key = 'CUST_GUEST_CHECKOUT'")
        ).scalar()
        assert guest_in_fact > 0

        guest_dim_row = conn.execute(
            text("SELECT is_guest, customer_segment FROM dim_customer WHERE customer_key = 'CUST_GUEST_CHECKOUT'")
        ).mappings().first()
        assert guest_dim_row is not None
        assert int(guest_dim_row["is_guest"]) == 1
        assert guest_dim_row["customer_segment"] == "Guest Checkout"

        registered_with_guest_id = conn.execute(
            text("SELECT COUNT(*) FROM dim_customer WHERE is_guest = 0 AND customer_id = 'GUEST_CHECKOUT'")
        ).scalar()
        assert registered_with_guest_id == 0

    freq_df = execute_named_report("order_frequency_distribution", engine=engine, config=cfg)
    assert not freq_df.empty
    assert "Guest Checkout" not in freq_df["frequency_bucket"].tolist()

    rfm_df = execute_named_report("customer_value_rfm", engine=engine, config=cfg)
    guest_rfm = rfm_df[rfm_df["customer_id"] == "GUEST_CHECKOUT"]
    assert len(guest_rfm) == 1
    assert int(guest_rfm.iloc[0]["is_guest"]) == 1
    assert guest_rfm.iloc[0]["customer_segment"] == "Guest Checkout"
