from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from retailflow.analytics import (
    ANALYTICS_QUERIES,
    execute_named_report,
    get_executive_kpi_summary,
    get_pipeline_runs_df,
    get_quality_checks_df,
    get_quarantine_records_df,
    get_quarantine_summary_df,
)
from retailflow.config import get_config
from retailflow.db import create_db_engine, initialize_warehouse, run_sql_query
from retailflow.pipeline import execute_retailflow_pipeline
from retailflow.sample_data import generate_synthetic_retail_batch


st.set_page_config(
    page_title="RetailFlow | Warehouse Operations & BI Workbench",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_STYLE = """
<style>
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1440px;
    }
    .rf-header-bar {
        background-color: #111827;
        border: 1px solid #1E293B;
        border-left: 4px solid #14B8A6;
        padding: 1rem 1.25rem;
        border-radius: 4px;
        margin-bottom: 1.25rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .rf-title {
        font-size: 1.35rem;
        font-weight: 700;
        color: #F8FAFC;
        letter-spacing: -0.02em;
        margin: 0;
    }
    .rf-subtitle {
        font-size: 0.85rem;
        color: #94A3B8;
        margin-top: 0.2rem;
    }
    .rf-badge {
        font-family: monospace;
        font-size: 0.78rem;
        padding: 0.25rem 0.65rem;
        border-radius: 3px;
        background-color: #0F292A;
        color: #2DD4BF;
        border: 1px solid #115E59;
    }
    .rf-kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(195px, 1fr));
        gap: 0.75rem;
        margin-bottom: 1.25rem;
    }
    .rf-kpi-card {
        background-color: #111827;
        border: 1px solid #1E293B;
        border-radius: 4px;
        padding: 0.85rem 1rem;
    }
    .rf-kpi-label {
        font-size: 0.74rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94A3B8;
        margin-bottom: 0.35rem;
    }
    .rf-kpi-value {
        font-size: 1.4rem;
        font-weight: 700;
        color: #F1F5F9;
        font-family: monospace;
    }
    .rf-kpi-meta {
        font-size: 0.75rem;
        color: #14B8A6;
        margin-top: 0.25rem;
    }
    .rf-section-title {
        font-size: 0.95rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #CBD5E1;
        border-bottom: 1px solid #1E293B;
        padding-bottom: 0.4rem;
        margin-top: 0.75rem;
        margin-bottom: 0.85rem;
    }
</style>
"""

st.markdown(CUSTOM_STYLE, unsafe_allow_html=True)

cfg = get_config()
engine = create_db_engine(cfg.db_url)
initialize_warehouse(engine, cfg)


def ensure_initial_demo_data() -> None:
    runs_df = get_pipeline_runs_df(engine, limit=1)
    if not runs_df.empty:
        return
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
    execute_retailflow_pipeline(
        csv_path=b1_path,
        batch_id="2026_q3_uk_eu_retail",
        engine=engine,
        config=cfg,
        execution_mode="airflow_scheduled",
    )
    execute_retailflow_pipeline(
        csv_path=b2_path,
        batch_id="2026_q4_oct_retail",
        engine=engine,
        config=cfg,
        execution_mode="airflow_scheduled",
    )


ensure_initial_demo_data()

with st.sidebar:
    st.markdown("### RETAILFLOW CONTROL PLANE")
    st.caption("Warehouse Ingestion, Quality Gating & Star-Schema Workbench")

    db_backend_label = "PostgreSQL Warehouse" if "postgres" in cfg.db_url else "Local SQLite Warehouse"
    st.markdown(f"**Storage Engine:** `{db_backend_label}`")
    st.markdown("**DAG ID:** `retailflow_daily_warehouse_etl`")
    st.divider()

    st.markdown("#### Trigger Batch Ingestion")
    trigger_mode = st.selectbox(
        "Batch Source",
        options=[
            "Generate Synthetic Batch (Incremental)",
            "Upload Custom CSV Batch",
            "Simulate High-Quarantine Failed Batch",
        ],
    )

    if trigger_mode == "Generate Synthetic Batch (Incremental)":
        batch_id_input = st.text_input("Batch Identifier", value="2026_q4_nov_incremental")
        seed_input = st.number_input("Deterministic Seed", min_value=1, max_value=9999, value=204)
        invoices_input = st.slider("Invoice Count", min_value=50, max_value=600, value=220, step=10)
        if st.button("Generate & Execute Pipeline", use_container_width=True):
            target_csv = cfg.raw_dir / f"{batch_id_input}.csv"
            generate_synthetic_retail_batch(
                output_path=target_csv,
                batch_name=batch_id_input,
                start_date="2026-10-15",
                days=30,
                num_invoices=int(invoices_input),
                seed=int(seed_input),
            )
            summary = execute_retailflow_pipeline(
                csv_path=target_csv,
                batch_id=batch_id_input,
                engine=engine,
                config=cfg,
                execution_mode="workbench_manual",
                raise_on_failure=False,
            )
            if summary.status == "SUCCESS":
                st.success(f"Run {summary.run_id} succeeded ({summary.valid_row_count} valid rows loaded).")
            else:
                st.error(f"Run {summary.run_id} failed at {summary.error_stage}: {summary.error_message}")
            st.rerun()

    elif trigger_mode == "Upload Custom CSV Batch":
        custom_batch_id = st.text_input("Custom Batch ID", value="custom_upload_batch")
        uploaded_file = st.file_uploader("Upload Retail CSV (UCI Schema)", type=["csv"])
        if uploaded_file is not None and st.button("Ingest Uploaded CSV", use_container_width=True):
            save_path = cfg.raw_dir / f"{custom_batch_id}.csv"
            save_path.write_bytes(uploaded_file.getvalue())
            summary = execute_retailflow_pipeline(
                csv_path=save_path,
                batch_id=custom_batch_id,
                engine=engine,
                config=cfg,
                execution_mode="workbench_upload",
                raise_on_failure=False,
            )
            if summary.status == "SUCCESS":
                st.success(f"Loaded {summary.valid_row_count} rows into star schema.")
            else:
                st.error(f"Failed at {summary.error_stage}: {summary.error_message}")
            st.rerun()

    else:
        fail_batch_id = st.text_input("Corrupt Batch ID", value="2026_q4_corrupt_feed")
        if st.button("Trigger Corrupt Batch (Verify Gate)", use_container_width=True):
            corrupt_path = cfg.raw_dir / f"{fail_batch_id}.csv"
            corrupt_path.write_text(
                "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
                "599001,85123A,WHITE HANGING HEART,0,2026-10-15 10:00:00,2.95,12346,United Kingdom\n"
                "599002,22423,REGENCY CAKESTAND,2,INVALID_DATE,-12.75,12347,United Kingdom\n"
                "599003,POST,POSTAGE,1,2026-10-15 11:00:00,15.00,12348,Germany\n"
                "599004,85099B,JUMBO BAG RED RETROSPOT,5,2026-10-15 12:00:00,2.08,12349,France\n",
                encoding="utf-8",
            )
            summary = execute_retailflow_pipeline(
                csv_path=corrupt_path,
                batch_id=fail_batch_id,
                engine=engine,
                config=cfg,
                execution_mode="workbench_fault_test",
                raise_on_failure=False,
            )
            st.warning(
                f"Gate halted promotion as expected ({summary.status} at {summary.error_stage}). "
                "Warehouse fact tables remain untainted."
            )
            st.rerun()

kpis = get_executive_kpi_summary(engine)
runs_df = get_pipeline_runs_df(engine, limit=25)
latest_status = str(runs_df.iloc[0]["status"]) if not runs_df.empty else "IDLE"

st.markdown(
    f"""
    <div class="rf-header-bar">
        <div>
            <div class="rf-title">RETAILFLOW // ANALYTICAL WAREHOUSE & PIPELINE WORKBENCH</div>
            <div class="rf-subtitle">Star-Schema Data Platform &bull; Idempotent Batch Ingestion &bull; Automated Quality Gates &bull; UCI Retail Standard</div>
        </div>
        <div class="rf-badge">LATEST RUN: {latest_status} &bull; {kpis['total_fact_lines']:,} FACT ROWS</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f"""
    <div class="rf-kpi-grid">
        <div class="rf-kpi-card">
            <div class="rf-kpi-label">Net Merchandise Revenue</div>
            <div class="rf-kpi-value">&pound;{kpis['net_revenue']:,.2f}</div>
            <div class="rf-kpi-meta">Gross &pound;{kpis['gross_revenue']:,.2f} less returns</div>
        </div>
        <div class="rf-kpi-card">
            <div class="rf-kpi-label">Completed Orders</div>
            <div class="rf-kpi-value">{kpis['completed_orders']:,}</div>
            <div class="rf-kpi-meta">AOV &pound;{kpis['avg_order_value']:,.2f} per order</div>
        </div>
        <div class="rf-kpi-card">
            <div class="rf-kpi-label">Return &amp; Cancellation Rate</div>
            <div class="rf-kpi-value">{kpis['return_rate_pct']:.2f}%</div>
            <div class="rf-kpi-meta">&pound;{kpis['return_amount']:,.2f} across {kpis['cancelled_orders']} credit invoices</div>
        </div>
        <div class="rf-kpi-card">
            <div class="rf-kpi-label">Identified Customers</div>
            <div class="rf-kpi-value">{kpis['active_customers']:,}</div>
            <div class="rf-kpi-meta">Guest checkouts isolated to GUEST_CHECKOUT</div>
        </div>
        <div class="rf-kpi-card">
            <div class="rf-kpi-label">Active SKUs in Catalog</div>
            <div class="rf-kpi-value">{kpis['active_products']:,}</div>
            <div class="rf-kpi-meta">Postage &amp; bank fees excluded</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

tab_ops, tab_dq, tab_bi, tab_sql = st.tabs(
    [
        "01 // PIPELINE RUN TELEMETRY",
        "02 // DATA QUALITY & QUARANTINE",
        "03 // MERCHANDISE & CUSTOMER BI",
        "04 // STAR-SCHEMA SQL INSPECTOR",
    ]
)

with tab_ops:
    st.markdown('<div class="rf-section-title">Pipeline Execution History (ops_pipeline_runs)</div>', unsafe_allow_html=True)
    if runs_df.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        failed_runs = runs_df[runs_df["status"] == "FAILED"]
        if not failed_runs.empty:
            latest_fail = failed_runs.iloc[0]
            st.error(
                f"**Most Recent Halted Run (`{latest_fail['run_id']}` | Batch `{latest_fail['batch_id']}`)**: "
                f"Halted at stage `{latest_fail['error_stage']}` — {latest_fail['error_message']}"
            )

        display_cols = [
            "run_id",
            "batch_id",
            "execution_mode",
            "status",
            "raw_row_count",
            "valid_row_count",
            "quarantined_row_count",
            "duplicate_row_count",
            "warehouse_fact_upserts",
            "quality_checks_passed",
            "quality_checks_failed",
            "duration_seconds",
            "started_at",
        ]
        st.dataframe(runs_df[display_cols], use_container_width=True, hide_index=True)

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="rf-section-title">Batch Row Disposition Breakdown</div>', unsafe_allow_html=True)
            fig_disp = go.Figure()
            fig_disp.add_trace(
                go.Bar(
                    name="Valid Staged Rows",
                    x=runs_df["batch_id"],
                    y=runs_df["valid_row_count"],
                    marker_color="#14B8A6",
                )
            )
            fig_disp.add_trace(
                go.Bar(
                    name="Deduplicated Rows",
                    x=runs_df["batch_id"],
                    y=runs_df["duplicate_row_count"],
                    marker_color="#F59E0B",
                )
            )
            fig_disp.add_trace(
                go.Bar(
                    name="Quarantined Rows",
                    x=runs_df["batch_id"],
                    y=runs_df["quarantined_row_count"],
                    marker_color="#EF4444",
                )
            )
            fig_disp.update_layout(
                barmode="stack",
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                paper_bgcolor="#111827",
                plot_bgcolor="#111827",
                font=dict(color="#CBD5E1", size=11),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_disp, use_container_width=True)

        with col_b:
            st.markdown('<div class="rf-section-title">Explicit Cleaning & Idempotency Rules</div>', unsafe_allow_html=True)
            rules_df = pd.DataFrame(
                [
                    {
                        "Anomaly / Edge Case": "Cancellations (InvoiceNo starts with 'C')",
                        "Pipeline Policy": "Flag is_cancelled=1, enforce negative quantity, record under return_amount to preserve Gross vs. Net revenue identity.",
                    },
                    {
                        "Anomaly / Edge Case": "Missing CustomerID",
                        "Pipeline Policy": "Map to CUST_GUEST_CHECKOUT (is_guest=1) so guest revenue is retained in fact_sales without polluting registered RFM cohorts.",
                    },
                    {
                        "Anomaly / Edge Case": "Exact Duplicate Line Items",
                        "Pipeline Policy": "Deduplicate within batch on deterministic SHA-256 signature; increment occurrence_count.",
                    },
                    {
                        "Anomaly / Edge Case": "Non-Merchandise Codes (POST, BANK CHARGES, etc.)",
                        "Pipeline Policy": "Quarantine to ops_quarantine_records with code NON_MERCHANDISE_CODE.",
                    },
                    {
                        "Anomaly / Edge Case": "Zero/Negative Price or Unparseable Date",
                        "Pipeline Policy": "Quarantine row with explicit rejection reason; block warehouse load if batch quarantine rate > 25%.",
                    },
                ]
            )
            st.dataframe(rules_df, use_container_width=True, hide_index=True)

with tab_dq:
    col_dq1, col_dq2 = st.columns([3, 2])
    with col_dq1:
        st.markdown('<div class="rf-section-title">Data Quality Gate Evaluations (ops_quality_checks)</div>', unsafe_allow_html=True)
        run_options = ["All Recent Runs"] + list(runs_df["run_id"].unique()) if not runs_df.empty else ["All Recent Runs"]
        selected_run = st.selectbox("Filter Quality Checks by Run ID", options=run_options)
        target_run_id = None if selected_run == "All Recent Runs" else selected_run
        checks_df = get_quality_checks_df(engine, run_id=target_run_id)
        if checks_df.empty:
            st.info("No quality checks found.")
        else:
            checks_df["status_label"] = checks_df["passed"].apply(lambda x: "PASS" if int(x) == 1 else "FAIL")
            st.dataframe(
                checks_df[
                    [
                        "status_label",
                        "stage",
                        "check_name",
                        "category",
                        "observed_value",
                        "threshold_expression",
                        "details",
                        "batch_id",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

    with col_dq2:
        st.markdown('<div class="rf-section-title">Quarantine Summary by Rejection Code</div>', unsafe_allow_html=True)
        q_sum_df = get_quarantine_summary_df(engine)
        if q_sum_df.empty:
            st.info("Zero quarantined records.")
        else:
            st.dataframe(q_sum_df, use_container_width=True, hide_index=True)

    st.markdown('<div class="rf-section-title">Quarantined Source Rows Inspector (ops_quarantine_records)</div>', unsafe_allow_html=True)
    quar_df = get_quarantine_records_df(engine, limit=100)
    st.dataframe(
        quar_df[
            [
                "batch_id",
                "source_row_number",
                "rejection_code",
                "rejection_reason",
                "invoice_no",
                "stock_code",
                "quantity_raw",
                "unit_price_raw",
                "invoice_date_raw",
                "customer_id_raw",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

with tab_bi:
    rev_df = execute_named_report("revenue_trends", engine=engine, config=cfg)
    prod_df = execute_named_report("best_selling_products", engine=engine, config=cfg)
    cust_df = execute_named_report("customer_value_rfm", engine=engine, config=cfg)
    freq_df = execute_named_report("order_frequency_distribution", engine=engine, config=cfg)
    country_df = execute_named_report("country_performance", engine=engine, config=cfg)

    st.markdown('<div class="rf-section-title">Daily Gross vs. Net Revenue & Return Leakage</div>', unsafe_allow_html=True)
    if not rev_df.empty:
        fig_rev = go.Figure()
        fig_rev.add_trace(
            go.Scatter(
                x=rev_df["transaction_date"],
                y=rev_df["net_revenue"],
                mode="lines",
                name="Net Revenue (GBP)",
                line=dict(color="#14B8A6", width=2.2),
                fill="tozeroy",
                fillcolor="rgba(20, 184, 166, 0.12)",
            )
        )
        fig_rev.add_trace(
            go.Bar(
                x=rev_df["transaction_date"],
                y=rev_df["return_value"],
                name="Returns & Cancellations (GBP)",
                marker_color="#EF4444",
                opacity=0.75,
            )
        )
        fig_rev.update_layout(
            height=340,
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor="#111827",
            plot_bgcolor="#111827",
            font=dict(color="#CBD5E1", size=11),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_rev, use_container_width=True)

    c_bi1, c_bi2 = st.columns(2)
    with c_bi1:
        st.markdown('<div class="rf-section-title">Top Performing Merchandise SKUs (dim_product)</div>', unsafe_allow_html=True)
        st.dataframe(prod_df.head(15), use_container_width=True, hide_index=True)

    with c_bi2:
        st.markdown('<div class="rf-section-title">Customer Order Frequency Cohorts (dim_customer)</div>', unsafe_allow_html=True)
        st.dataframe(freq_df, use_container_width=True, hide_index=True)
        st.markdown('<div class="rf-section-title">Geographic Revenue & Return Rate (fact_sales)</div>', unsafe_allow_html=True)
        st.dataframe(country_df, use_container_width=True, hide_index=True)

    st.markdown('<div class="rf-section-title">High-Value Customer Accounts & RFM Segments</div>', unsafe_allow_html=True)
    registered_cust_df = cust_df[cust_df["is_guest"] == 0]
    st.dataframe(registered_cust_df.head(20), use_container_width=True, hide_index=True)

with tab_sql:
    st.markdown('<div class="rf-section-title">Version-Controlled Analytical SQL Library</div>', unsafe_allow_html=True)
    selected_query_key = st.selectbox(
        "Select Analytical SQL Model",
        options=list(ANALYTICS_QUERIES.keys()),
    )
    sql_file_path = cfg.sql_dir / "analytics" / ANALYTICS_QUERIES[selected_query_key]
    sql_source = sql_file_path.read_text(encoding="utf-8")

    col_s1, col_s2 = st.columns([2, 3])
    with col_s1:
        st.code(sql_source, language="sql")
    with col_s2:
        result_df = run_sql_query(sql_source, engine=engine)
        st.dataframe(result_df, use_container_width=True, hide_index=True)
