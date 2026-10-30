import pandas as pd
from retailflow.config import RetailFlowConfig, get_config
from retailflow.db import Engine, create_db_engine, run_sql_file, text


ANALYTICS_QUERIES = {
    "revenue_trends": "revenue_trends.sql",
    "best_selling_products": "best_selling_products.sql",
    "customer_value_rfm": "customer_value_rfm.sql",
    "order_frequency_distribution": "order_frequency_distribution.sql",
    "country_performance": "country_performance.sql",
    "cancellation_and_guest_audit": "cancellation_and_guest_audit.sql",
}


def execute_named_report(
    report_name: str,
    engine: Engine | None = None,
    config: RetailFlowConfig | None = None,
) -> pd.DataFrame:
    if report_name not in ANALYTICS_QUERIES:
        raise KeyError(f"Unknown report '{report_name}'. Valid options: {list(ANALYTICS_QUERIES.keys())}")
    cfg = config or get_config()
    eng = engine or create_db_engine(cfg.db_url)
    sql_path = cfg.sql_dir / "analytics" / ANALYTICS_QUERIES[report_name]
    return run_sql_file(sql_path, engine=eng)


def get_executive_kpi_summary(engine: Engine | None = None) -> dict[str, float | int]:
    eng = engine or create_db_engine()
    with eng.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total_fact_lines,
                    COUNT(DISTINCT CASE WHEN is_cancelled = 0 AND is_return = 0 THEN invoice_no END) AS completed_orders,
                    COUNT(DISTINCT CASE WHEN is_cancelled = 1 OR is_return = 1 THEN invoice_no END) AS cancelled_orders,
                    COALESCE(SUM(gross_amount), 0.0) AS gross_revenue,
                    COALESCE(SUM(return_amount), 0.0) AS return_amount,
                    COALESCE(SUM(net_amount), 0.0) AS net_revenue
                FROM fact_sales
                """
            )
        ).mappings().first()

        cust_count = int(
            conn.execute(
                text("SELECT COUNT(*) FROM dim_customer WHERE is_guest = 0")
            ).scalar()
            or 0
        )
        prod_count = int(
            conn.execute(text("SELECT COUNT(*) FROM dim_product")).scalar() or 0
        )

    if not row:
        return {
            "total_fact_lines": 0,
            "completed_orders": 0,
            "cancelled_orders": 0,
            "gross_revenue": 0.0,
            "return_amount": 0.0,
            "net_revenue": 0.0,
            "avg_order_value": 0.0,
            "return_rate_pct": 0.0,
            "active_customers": 0,
            "active_products": 0,
        }

    completed = int(row["completed_orders"] or 0)
    gross = float(row["gross_revenue"] or 0.0)
    ret = float(row["return_amount"] or 0.0)
    net = float(row["net_revenue"] or 0.0)
    aov = (net / completed) if completed > 0 else 0.0
    ret_rate = (ret / gross * 100.0) if gross > 0 else 0.0

    return {
        "total_fact_lines": int(row["total_fact_lines"] or 0),
        "completed_orders": completed,
        "cancelled_orders": int(row["cancelled_orders"] or 0),
        "gross_revenue": round(gross, 2),
        "return_amount": round(ret, 2),
        "net_revenue": round(net, 2),
        "avg_order_value": round(aov, 2),
        "return_rate_pct": round(ret_rate, 2),
        "active_customers": cust_count,
        "active_products": prod_count,
    }


def get_pipeline_runs_df(engine: Engine | None = None, limit: int = 50) -> pd.DataFrame:
    eng = engine or create_db_engine()
    with eng.connect() as conn:
        return conn.to_dataframe(
            text(
                """
                SELECT
                    run_id, batch_id, dag_id, execution_mode, status,
                    started_at, finished_at, duration_seconds, source_file,
                    raw_row_count, valid_row_count, quarantined_row_count,
                    duplicate_row_count, warehouse_fact_upserts,
                    quality_checks_total, quality_checks_passed,
                    quality_checks_failed, error_stage, error_message
                FROM ops_pipeline_runs
                ORDER BY started_at DESC
                LIMIT :limit
                """
            ),
            params={"limit": limit},
        )


def get_quality_checks_df(
    engine: Engine | None = None,
    run_id: str | None = None,
    limit: int = 200,
) -> pd.DataFrame:
    eng = engine or create_db_engine()
    with eng.connect() as conn:
        if run_id:
            return conn.to_dataframe(
                text(
                    """
                    SELECT
                        check_id, run_id, batch_id, stage, check_name,
                        category, severity, passed, observed_value,
                        threshold_expression, details, evaluated_at
                    FROM ops_quality_checks
                    WHERE run_id = :run_id
                    ORDER BY stage ASC, check_name ASC
                    """
                ),
                params={"run_id": run_id},
            )
        return conn.to_dataframe(
            text(
                """
                SELECT
                    check_id, run_id, batch_id, stage, check_name,
                    category, severity, passed, observed_value,
                    threshold_expression, details, evaluated_at
                FROM ops_quality_checks
                ORDER BY evaluated_at DESC
                LIMIT :limit
                """
            ),
            params={"limit": limit},
        )


def get_quarantine_summary_df(engine: Engine | None = None) -> pd.DataFrame:
    eng = engine or create_db_engine()
    with eng.connect() as conn:
        return conn.to_dataframe(
            text(
                """
                SELECT
                    batch_id,
                    rejection_code,
                    COUNT(*) AS rejected_rows,
                    MIN(rejection_reason) AS sample_reason
                FROM ops_quarantine_records
                GROUP BY batch_id, rejection_code
                ORDER BY rejected_rows DESC
                """
            ),
        )


def get_quarantine_records_df(
    engine: Engine | None = None,
    batch_id: str | None = None,
    limit: int = 250,
) -> pd.DataFrame:
    eng = engine or create_db_engine()
    with eng.connect() as conn:
        if batch_id:
            return conn.to_dataframe(
                text(
                    """
                    SELECT
                        quarantine_id, run_id, batch_id, source_row_number,
                        invoice_no, stock_code, description, quantity_raw,
                        invoice_date_raw, unit_price_raw, customer_id_raw,
                        country_raw, rejection_code, rejection_reason, quarantined_at
                    FROM ops_quarantine_records
                    WHERE batch_id = :batch_id
                    ORDER BY source_row_number ASC
                    LIMIT :limit
                    """
                ),
                params={"batch_id": batch_id, "limit": limit},
            )
        return conn.to_dataframe(
            text(
                """
                SELECT
                    quarantine_id, run_id, batch_id, source_row_number,
                    invoice_no, stock_code, description, quantity_raw,
                    invoice_date_raw, unit_price_raw, customer_id_raw,
                    country_raw, rejection_code, rejection_reason, quarantined_at
                FROM ops_quarantine_records
                ORDER BY quarantined_at DESC, source_row_number ASC
                LIMIT :limit
                """
            ),
            params={"limit": limit},
        )
