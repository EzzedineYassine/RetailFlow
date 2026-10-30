from dataclasses import dataclass
from datetime import datetime, timezone
from retailflow.config import RetailFlowConfig, get_config
from retailflow.db import Engine, text


class DataQualityGateError(RuntimeError):
    pass


@dataclass(frozen=True)
class QualityCheckRecord:
    check_id: str
    run_id: str
    batch_id: str
    stage: str
    check_name: str
    category: str
    severity: str
    passed: int
    observed_value: float
    threshold_expression: str
    details: str
    evaluated_at: str


def persist_quality_checks(checks: list[QualityCheckRecord], engine: Engine) -> None:
    if not checks:
        return
    rows = [
        {
            "check_id": c.check_id,
            "run_id": c.run_id,
            "batch_id": c.batch_id,
            "stage": c.stage,
            "check_name": c.check_name,
            "category": c.category,
            "severity": c.severity,
            "passed": c.passed,
            "observed_value": c.observed_value,
            "threshold_expression": c.threshold_expression,
            "details": c.details,
            "evaluated_at": c.evaluated_at,
        }
        for c in checks
    ]
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text("DELETE FROM ops_quality_checks WHERE check_id = :check_id"),
                {"check_id": row["check_id"]},
            )
        conn.execute(
            text(
                """
                INSERT INTO ops_quality_checks (
                    check_id, run_id, batch_id, stage, check_name, category,
                    severity, passed, observed_value, threshold_expression,
                    details, evaluated_at
                ) VALUES (
                    :check_id, :run_id, :batch_id, :stage, :check_name, :category,
                    :severity, :passed, :observed_value, :threshold_expression,
                    :details, :evaluated_at
                )
                """
            ),
            rows,
        )


def run_staging_quality_checks(
    batch_id: str,
    run_id: str,
    engine: Engine,
    config: RetailFlowConfig | None = None,
) -> list[QualityCheckRecord]:
    cfg = config or get_config()
    now_iso = datetime.now(timezone.utc).isoformat()
    checks: list[QualityCheckRecord] = []

    with engine.connect() as conn:
        raw_total = int(
            conn.execute(
                text("SELECT COUNT(*) FROM raw_transactions WHERE batch_id = :b"),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        stg_total = int(
            conn.execute(
                text("SELECT COUNT(*) FROM stg_transactions WHERE batch_id = :b"),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        quar_total = int(
            conn.execute(
                text("SELECT COUNT(*) FROM ops_quarantine_records WHERE batch_id = :b"),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        null_critical = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM stg_transactions
                    WHERE batch_id = :b
                      AND (
                          invoice_no IS NULL OR invoice_no = ''
                          OR stock_code IS NULL OR stock_code = ''
                          OR customer_id IS NULL OR customer_id = ''
                          OR date_key IS NULL
                      )
                    """
                ),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        non_pos_prices = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM stg_transactions
                    WHERE batch_id = :b AND (unit_price <= 0 OR quantity = 0)
                    """
                ),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        dup_stg_keys = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*) - COUNT(DISTINCT stg_line_id)
                    FROM stg_transactions
                    WHERE batch_id = :b
                    """
                ),
                {"b": batch_id},
            ).scalar()
            or 0
        )

    quarantine_rate = (quar_total / raw_total) if raw_total > 0 else 1.0

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_STG_NON_EMPTY",
            run_id=run_id,
            batch_id=batch_id,
            stage="STAGING",
            check_name="staging_batch_non_empty",
            category="Completeness",
            severity="CRITICAL",
            passed=1 if stg_total > 0 else 0,
            observed_value=float(stg_total),
            threshold_expression="> 0 rows",
            details=f"Staging produced {stg_total} valid rows out of {raw_total} raw rows.",
            evaluated_at=now_iso,
        )
    )

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_STG_QUARANTINE_RATE",
            run_id=run_id,
            batch_id=batch_id,
            stage="STAGING",
            check_name="quarantine_rate_within_bounds",
            category="Validity",
            severity="CRITICAL",
            passed=1 if quarantine_rate <= cfg.max_quarantine_rate else 0,
            observed_value=round(quarantine_rate * 100.0, 2),
            threshold_expression=f"<= {cfg.max_quarantine_rate * 100:.1f}%",
            details=f"Quarantined {quar_total} of {raw_total} rows ({quarantine_rate * 100:.2f}%).",
            evaluated_at=now_iso,
        )
    )

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_STG_CRITICAL_NULLS",
            run_id=run_id,
            batch_id=batch_id,
            stage="STAGING",
            check_name="zero_null_critical_identifiers",
            category="Null Rates",
            severity="CRITICAL",
            passed=1 if null_critical == 0 else 0,
            observed_value=float(null_critical),
            threshold_expression="== 0 rows",
            details=f"Found {null_critical} rows with null/blank invoice, stock_code, customer_id, or date_key.",
            evaluated_at=now_iso,
        )
    )

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_STG_DOMAIN_BOUNDS",
            run_id=run_id,
            batch_id=batch_id,
            stage="STAGING",
            check_name="strictly_positive_price_nonzero_qty",
            category="Suspicious Values",
            severity="CRITICAL",
            passed=1 if non_pos_prices == 0 else 0,
            observed_value=float(non_pos_prices),
            threshold_expression="== 0 rows",
            details=f"Found {non_pos_prices} staging rows with unit_price <= 0 or quantity == 0.",
            evaluated_at=now_iso,
        )
    )

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_STG_UNIQUE_KEY",
            run_id=run_id,
            batch_id=batch_id,
            stage="STAGING",
            check_name="staging_natural_key_uniqueness",
            category="Uniqueness",
            severity="CRITICAL",
            passed=1 if dup_stg_keys == 0 else 0,
            observed_value=float(dup_stg_keys),
            threshold_expression="== 0 duplicates",
            details=f"Duplicate staging line keys in batch: {dup_stg_keys}.",
            evaluated_at=now_iso,
        )
    )

    persist_quality_checks(checks, engine)

    failed_critical = [c for c in checks if c.passed == 0 and c.severity == "CRITICAL"]
    if failed_critical:
        summary = "; ".join(f"{c.check_name} ({c.details})" for c in failed_critical)
        raise DataQualityGateError(f"Staging quality gate failed: {summary}")

    return checks


def run_warehouse_quality_checks(
    batch_id: str,
    run_id: str,
    engine: Engine,
) -> list[QualityCheckRecord]:
    now_iso = datetime.now(timezone.utc).isoformat()
    checks: list[QualityCheckRecord] = []

    with engine.connect() as conn:
        orphan_products = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM fact_sales f
                    LEFT JOIN dim_product p ON f.product_key = p.product_key
                    WHERE p.product_key IS NULL
                    """
                )
            ).scalar()
            or 0
        )
        orphan_customers = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM fact_sales f
                    LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
                    WHERE c.customer_key IS NULL
                    """
                )
            ).scalar()
            or 0
        )
        orphan_dates = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM fact_sales f
                    LEFT JOIN dim_date d ON f.date_key = d.date_key
                    WHERE d.date_key IS NULL
                    """
                )
            ).scalar()
            or 0
        )
        accounting_mismatch = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM fact_sales
                    WHERE ABS((gross_amount - return_amount) - net_amount) > 0.01
                    """
                )
            ).scalar()
            or 0
        )
        stg_batch_count = int(
            conn.execute(
                text("SELECT COUNT(*) FROM stg_transactions WHERE batch_id = :b"),
                {"b": batch_id},
            ).scalar()
            or 0
        )
        fact_batch_count = int(
            conn.execute(
                text("SELECT COUNT(*) FROM fact_sales WHERE batch_id = :b"),
                {"b": batch_id},
            ).scalar()
            or 0
        )

    total_orphans = orphan_products + orphan_customers + orphan_dates
    reconciled_diff = abs(stg_batch_count - fact_batch_count)

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_WH_REF_INTEGRITY",
            run_id=run_id,
            batch_id=batch_id,
            stage="WAREHOUSE",
            check_name="star_schema_referential_integrity",
            category="Referential Integrity",
            severity="CRITICAL",
            passed=1 if total_orphans == 0 else 0,
            observed_value=float(total_orphans),
            threshold_expression="== 0 orphan FKs",
            details=(
                f"Orphan foreign keys across fact_sales: products={orphan_products}, "
                f"customers={orphan_customers}, dates={orphan_dates}."
            ),
            evaluated_at=now_iso,
        )
    )

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_WH_NET_REVENUE_IDENTITY",
            run_id=run_id,
            batch_id=batch_id,
            stage="WAREHOUSE",
            check_name="net_revenue_accounting_identity",
            category="Validity",
            severity="CRITICAL",
            passed=1 if accounting_mismatch == 0 else 0,
            observed_value=float(accounting_mismatch),
            threshold_expression="== 0 mismatched rows",
            details=f"Rows violating (gross_amount - return_amount == net_amount): {accounting_mismatch}.",
            evaluated_at=now_iso,
        )
    )

    checks.append(
        QualityCheckRecord(
            check_id=f"{run_id}_WH_ROW_RECONCILIATION",
            run_id=run_id,
            batch_id=batch_id,
            stage="WAREHOUSE",
            check_name="staging_to_fact_row_reconciliation",
            category="Completeness",
            severity="CRITICAL",
            passed=1 if reconciled_diff == 0 else 0,
            observed_value=float(reconciled_diff),
            threshold_expression="== 0 row delta",
            details=f"Staging rows={stg_batch_count}, Fact rows={fact_batch_count} for batch {batch_id}.",
            evaluated_at=now_iso,
        )
    )

    persist_quality_checks(checks, engine)

    failed_critical = [c for c in checks if c.passed == 0 and c.severity == "CRITICAL"]
    if failed_critical:
        summary = "; ".join(f"{c.check_name} ({c.details})" for c in failed_critical)
        raise DataQualityGateError(f"Warehouse quality gate failed: {summary}")

    return checks
