from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

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
from retailflow.db import create_db_engine, initialize_warehouse
from retailflow.pipeline import execute_retailflow_pipeline
from retailflow.sample_data import generate_synthetic_retail_batch


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_seeded()
    yield


app = FastAPI(
    title="RetailFlow Operations & BI Workbench",
    version="1.0.0",
    lifespan=lifespan,
)


class GenerateBatchRequest(BaseModel):
    batch_id: str = "2026_q4_nov_incremental"
    start_date: str = "2026-10-15"
    days: int = 30
    invoices: int = 200
    seed: int = 204


class CorruptBatchRequest(BaseModel):
    batch_id: str = "2026_q4_corrupt_feed"


def ensure_seeded() -> None:
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    initialize_warehouse(engine, cfg)
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


@app.get("/api/overview")
def api_overview():
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    ensure_seeded()
    kpis = get_executive_kpi_summary(engine)
    runs = get_pipeline_runs_df(engine, limit=25).to_dict(orient="records")
    quar_summary = get_quarantine_summary_df(engine).to_dict(orient="records")
    return {
        "storage_engine": "PostgreSQL Warehouse" if "postgres" in cfg.db_url else "SQLite Warehouse",
        "dag_id": "retailflow_daily_warehouse_etl",
        "kpis": kpis,
        "runs": runs,
        "quarantine_summary": quar_summary,
    }


@app.get("/api/quality-checks")
def api_quality_checks(run_id: str | None = None):
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    df = get_quality_checks_df(engine, run_id=run_id if run_id else None, limit=200)
    return {"checks": df.to_dict(orient="records")}


@app.get("/api/quarantine-records")
def api_quarantine_records(batch_id: str | None = None):
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    df = get_quarantine_records_df(engine, batch_id=batch_id if batch_id else None, limit=150)
    return {"records": df.to_dict(orient="records")}


@app.get("/api/reports/{report_name}")
def api_report(report_name: str):
    if report_name not in ANALYTICS_QUERIES:
        raise HTTPException(status_code=404, detail=f"Unknown report: {report_name}")
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    sql_path = cfg.sql_dir / "analytics" / ANALYTICS_QUERIES[report_name]
    sql_text = sql_path.read_text(encoding="utf-8")
    df = execute_named_report(report_name, engine=engine, config=cfg)
    return {
        "report_name": report_name,
        "sql": sql_text,
        "rows": df.to_dict(orient="records"),
    }


@app.post("/api/trigger/synthetic")
def api_trigger_synthetic(req: GenerateBatchRequest):
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    csv_path = cfg.raw_dir / f"{req.batch_id}.csv"
    generate_synthetic_retail_batch(
        output_path=csv_path,
        batch_name=req.batch_id,
        start_date=req.start_date,
        days=req.days,
        num_invoices=req.invoices,
        seed=req.seed,
    )
    summary = execute_retailflow_pipeline(
        csv_path=csv_path,
        batch_id=req.batch_id,
        engine=engine,
        config=cfg,
        execution_mode="workbench_manual",
        raise_on_failure=False,
    )
    return summary.__dict__


@app.post("/api/trigger/corrupt")
def api_trigger_corrupt(req: CorruptBatchRequest):
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    corrupt_path = cfg.raw_dir / f"{req.batch_id}.csv"
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
        batch_id=req.batch_id,
        engine=engine,
        config=cfg,
        execution_mode="workbench_fault_test",
        raise_on_failure=False,
    )
    return summary.__dict__


@app.post("/api/trigger/upload")
async def api_trigger_upload(
    batch_id: str = Form(...),
    file: UploadFile = File(...),
):
    cfg = get_config()
    engine = create_db_engine(cfg.db_url)
    save_path = cfg.raw_dir / f"{batch_id}.csv"
    content = await file.read()
    save_path.write_bytes(content)
    summary = execute_retailflow_pipeline(
        csv_path=save_path,
        batch_id=batch_id,
        engine=engine,
        config=cfg,
        execution_mode="workbench_upload",
        raise_on_failure=False,
    )
    return summary.__dict__


WORKBENCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RetailFlow // Warehouse Operations & BI Workbench</title>
<style>
  :root {
    --bg-base: #0b0f17;
    --bg-panel: #111827;
    --bg-elevated: #172033;
    --border-subtle: #1e293b;
    --border-strong: #334155;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --accent-teal: #14b8a6;
    --accent-teal-dim: rgba(20, 184, 166, 0.14);
    --status-pass: #10b981;
    --status-warn: #f59e0b;
    --status-fail: #ef4444;
    --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    --font-mono: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background-color: var(--bg-base);
    color: var(--text-primary);
    font-family: var(--font-sans);
    font-size: 13.5px;
    line-height: 1.45;
  }
  .layout {
    display: grid;
    grid-template-columns: 290px 1fr;
    min-height: 100vh;
  }
  @media (max-width: 980px) {
    .layout { grid-template-columns: 1fr; }
  }
  .sidebar {
    background-color: var(--bg-panel);
    border-right: 1px solid var(--border-subtle);
    padding: 1.25rem;
    display: flex;
    flex-direction: column;
    gap: 1.2rem;
  }
  .brand-box {
    border-left: 3px solid var(--accent-teal);
    padding-left: 0.75rem;
  }
  .brand-title {
    font-weight: 700;
    font-size: 1.05rem;
    letter-spacing: 0.04em;
    color: var(--text-primary);
  }
  .brand-sub {
    font-size: 0.76rem;
    color: var(--text-secondary);
    margin-top: 2px;
  }
  .meta-kv {
    background: var(--bg-base);
    border: 1px solid var(--border-subtle);
    border-radius: 4px;
    padding: 0.65rem 0.75rem;
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--text-secondary);
  }
  .meta-kv div { margin-bottom: 4px; }
  .meta-kv span { color: var(--accent-teal); }
  .control-card {
    background: var(--bg-base);
    border: 1px solid var(--border-subtle);
    border-radius: 4px;
    padding: 0.85rem;
  }
  .control-title {
    font-size: 0.76rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
    margin-bottom: 0.65rem;
  }
  label {
    display: block;
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin-bottom: 0.25rem;
    margin-top: 0.55rem;
  }
  input {
    width: 100%;
    background: var(--bg-panel);
    border: 1px solid var(--border-strong);
    color: var(--text-primary);
    padding: 0.42rem 0.55rem;
    border-radius: 3px;
    font-family: var(--font-mono);
    font-size: 0.78rem;
  }
  input:focus {
    outline: 1px solid var(--accent-teal);
    border-color: var(--accent-teal);
  }
  .btn {
    width: 100%;
    margin-top: 0.75rem;
    padding: 0.5rem 0.75rem;
    border-radius: 3px;
    font-weight: 600;
    font-size: 0.78rem;
    cursor: pointer;
    border: 1px solid transparent;
    transition: opacity 0.15s;
  }
  .btn-primary {
    background-color: var(--accent-teal);
    color: #042f2e;
  }
  .btn-danger {
    background-color: rgba(239, 68, 68, 0.15);
    color: #fca5a5;
    border-color: #7f1d1d;
  }
  .btn:hover { opacity: 0.9; }
  .main {
    padding: 1.25rem 1.6rem;
    overflow-x: hidden;
  }
  .topbar {
    background: var(--bg-panel);
    border: 1px solid var(--border-subtle);
    border-left: 4px solid var(--accent-teal);
    padding: 0.9rem 1.2rem;
    border-radius: 4px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
  }
  .topbar-title {
    font-size: 1.15rem;
    font-weight: 700;
  }
  .topbar-sub {
    font-size: 0.8rem;
    color: var(--text-secondary);
    margin-top: 2px;
  }
  .status-pill {
    font-family: var(--font-mono);
    font-size: 0.76rem;
    padding: 0.3rem 0.65rem;
    border-radius: 3px;
    background: var(--accent-teal-dim);
    color: var(--accent-teal);
    border: 1px solid #115e59;
  }
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(185px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1.15rem;
  }
  .kpi-card {
    background: var(--bg-panel);
    border: 1px solid var(--border-subtle);
    border-radius: 4px;
    padding: 0.85rem 1rem;
  }
  .kpi-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
  }
  .kpi-val {
    font-family: var(--font-mono);
    font-size: 1.35rem;
    font-weight: 700;
    margin-top: 0.3rem;
    color: var(--text-primary);
  }
  .kpi-meta {
    font-size: 0.74rem;
    color: var(--accent-teal);
    margin-top: 0.2rem;
  }
  .tabs {
    display: flex;
    gap: 0.4rem;
    border-bottom: 1px solid var(--border-subtle);
    margin-bottom: 1rem;
  }
  .tab-btn {
    background: transparent;
    color: var(--text-secondary);
    border: none;
    border-bottom: 2px solid transparent;
    padding: 0.6rem 0.95rem;
    font-size: 0.8rem;
    font-weight: 600;
    cursor: pointer;
    letter-spacing: 0.03em;
  }
  .tab-btn.active {
    color: var(--accent-teal);
    border-bottom-color: var(--accent-teal);
    background: rgba(20, 184, 166, 0.06);
  }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }
  .section-hdr {
    font-size: 0.82rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #cbd5e1;
    margin: 0.75rem 0 0.6rem 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .table-wrap {
    background: var(--bg-panel);
    border: 1px solid var(--border-subtle);
    border-radius: 4px;
    overflow-x: auto;
    margin-bottom: 1rem;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.79rem;
  }
  th {
    background: #0f172a;
    color: var(--text-secondary);
    text-align: left;
    font-weight: 600;
    padding: 0.55rem 0.75rem;
    border-bottom: 1px solid var(--border-subtle);
    white-space: nowrap;
  }
  td {
    padding: 0.5rem 0.75rem;
    border-bottom: 1px solid #161f30;
    font-family: var(--font-mono);
    font-size: 0.77rem;
    color: #e2e8f0;
    white-space: nowrap;
  }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .badge-pass {
    background: rgba(16, 185, 129, 0.15);
    color: #34d399;
    padding: 2px 6px;
    border-radius: 3px;
    font-weight: 600;
  }
  .badge-fail {
    background: rgba(239, 68, 68, 0.18);
    color: #f87171;
    padding: 2px 6px;
    border-radius: 3px;
    font-weight: 600;
  }
  .alert-banner {
    background: rgba(239, 68, 68, 0.12);
    border: 1px solid #7f1d1d;
    border-left: 4px solid var(--status-fail);
    color: #fecaca;
    padding: 0.7rem 0.95rem;
    border-radius: 4px;
    margin-bottom: 0.9rem;
    font-size: 0.8rem;
  }
  .toast {
    margin-top: 0.6rem;
    padding: 0.5rem 0.65rem;
    border-radius: 3px;
    font-size: 0.76rem;
    display: none;
  }
  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }
  @media (max-width: 1100px) {
    .grid-2 { grid-template-columns: 1fr; }
  }
  pre.sql-box {
    background: #090d14;
    border: 1px solid var(--border-subtle);
    border-radius: 4px;
    padding: 0.85rem;
    font-family: var(--font-mono);
    font-size: 0.76rem;
    color: #5eead4;
    overflow-x: auto;
    line-height: 1.5;
  }
  select {
    background: var(--bg-panel);
    color: var(--text-primary);
    border: 1px solid var(--border-strong);
    padding: 0.35rem 0.6rem;
    border-radius: 3px;
    font-size: 0.78rem;
  }
</style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <div class="brand-box">
      <div class="brand-title">RETAILFLOW</div>
      <div class="brand-sub">Warehouse Operations &amp; BI Workbench</div>
    </div>

    <div class="meta-kv">
      <div>ENGINE: <span id="meta-engine">Loading...</span></div>
      <div>DAG ID: <span id="meta-dag">retailflow_daily_warehouse_etl</span></div>
      <div>SCHEMA: <span>Star Schema (4 Layers)</span></div>
    </div>

    <div class="control-card">
      <div class="control-title">Ingest Synthetic Batch</div>
      <label for="inp-batch-id">Batch Identifier</label>
      <input id="inp-batch-id" type="text" value="2026_q4_nov_incremental" />
      <label for="inp-invoices">Invoice Volume</label>
      <input id="inp-invoices" type="number" value="180" min="20" max="600" />
      <label for="inp-seed">Deterministic Seed</label>
      <input id="inp-seed" type="number" value="204" min="1" max="9999" />
      <button class="btn btn-primary" onclick="triggerSyntheticBatch()">Run Idempotent ETL Batch</button>
    </div>

    <div class="control-card">
      <div class="control-title">Fault-Injection Gate Test</div>
      <p style="font-size:0.74rem; color:var(--text-secondary);">
        Simulates a corrupt feed (&gt;25% quarantine rate) to verify that the Staging Quality Gate halts execution and protects warehouse tables.
      </p>
      <button class="btn btn-danger" onclick="triggerCorruptBatch()">Simulate Corrupt Feed</button>
    </div>

    <div id="action-toast" class="toast"></div>
  </aside>

  <main class="main">
    <div class="topbar">
      <div>
        <div class="topbar-title">RETAILFLOW // DATA ENGINEERING &amp; STAR-SCHEMA WORKBENCH</div>
        <div class="topbar-sub">Automated CSV Ingestion &bull; Explicit Anomaly Quarantine &bull; Quality Gates &bull; Kimball Warehouse</div>
      </div>
      <div class="status-pill" id="top-status-pill">SYNCING...</div>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Net Merchandise Revenue</div>
        <div class="kpi-val" id="kpi-net-rev">&pound;0.00</div>
        <div class="kpi-meta" id="kpi-gross-rev">Gross &pound;0.00</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Completed Orders</div>
        <div class="kpi-val" id="kpi-orders">0</div>
        <div class="kpi-meta" id="kpi-aov">AOV &pound;0.00</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Return &amp; Cancellation Rate</div>
        <div class="kpi-val" id="kpi-ret-rate">0.00%</div>
        <div class="kpi-meta" id="kpi-ret-amt">&pound;0.00 credit leakage</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Identified Customers</div>
        <div class="kpi-val" id="kpi-customers">0</div>
        <div class="kpi-meta">Guest orders isolated</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Active Catalog SKUs</div>
        <div class="kpi-val" id="kpi-products">0</div>
        <div class="kpi-meta">Postage/Fees quarantined</div>
      </div>
    </div>

    <div class="tabs">
      <button class="tab-btn active" onclick="switchTab('ops', this)">01 // PIPELINE TELEMETRY</button>
      <button class="tab-btn" onclick="switchTab('dq', this)">02 // DATA QUALITY &amp; QUARANTINE</button>
      <button class="tab-btn" onclick="switchTab('bi', this)">03 // MERCHANDISE &amp; CUSTOMER BI</button>
      <button class="tab-btn" onclick="switchTab('sql', this)">04 // SQL ANALYTICS INSPECTOR</button>
    </div>

    <section id="tab-ops" class="tab-panel active">
      <div id="failed-run-banner"></div>
      <div class="section-hdr">
        <span>Pipeline Run Audit Log (ops_pipeline_runs)</span>
      </div>
      <div class="table-wrap">
        <table id="tbl-runs">
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Batch ID</th>
              <th>Mode</th>
              <th>Status</th>
              <th>Raw Rows</th>
              <th>Valid Staged</th>
              <th>Quarantined</th>
              <th>Duplicates</th>
              <th>Fact Upserts</th>
              <th>DQ Passed</th>
              <th>DQ Failed</th>
              <th>Duration (s)</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="section-hdr"><span>Explicit Anomaly &amp; Idempotency Engineering Policies</span></div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Retail Edge Case</th><th>Layer</th><th>Deterministic Rule</th></tr>
          </thead>
          <tbody>
            <tr><td>Cancelled Invoices (Prefix 'C')</td><td>Staging &amp; Fact</td><td>Flag is_cancelled=1, enforce negative quantity, route value to return_amount so gross sales are never inflated.</td></tr>
            <tr><td>Missing CustomerID</td><td>Staging &amp; Dim</td><td>Map to CUST_GUEST_CHECKOUT (is_guest=1) to preserve store revenue while keeping registered RFM cohorts pure.</td></tr>
            <tr><td>Exact Duplicate Line Transmissions</td><td>Staging</td><td>Deduplicate within batch via SHA-256 natural key signature; increment occurrence_count.</td></tr>
            <tr><td>Non-Merchandise Codes (POST, BANK CHARGES)</td><td>Quarantine</td><td>Quarantine into ops_quarantine_records (NON_MERCHANDISE_CODE) to prevent postage from skewing SKU metrics.</td></tr>
            <tr><td>Invalid Price (&lt;=0), Zero Qty, Bad Date</td><td>Quarantine &amp; Gate</td><td>Quarantine with explicit rejection reason; halt warehouse promotion if batch quarantine exceeds 25%.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="tab-dq" class="tab-panel">
      <div class="grid-2">
        <div>
          <div class="section-hdr"><span>Quality Gate Evaluations (ops_quality_checks)</span></div>
          <div class="table-wrap">
            <table id="tbl-checks">
              <thead>
                <tr>
                  <th>Result</th>
                  <th>Stage</th>
                  <th>Check Name</th>
                  <th>Observed</th>
                  <th>Threshold</th>
                  <th>Batch ID</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div>
          <div class="section-hdr"><span>Quarantine Breakdown by Rejection Code</span></div>
          <div class="table-wrap">
            <table id="tbl-quar-summary">
              <thead>
                <tr>
                  <th>Batch ID</th>
                  <th>Rejection Code</th>
                  <th>Rows</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="section-hdr"><span>Quarantined Source Records (ops_quarantine_records)</span></div>
      <div class="table-wrap">
        <table id="tbl-quar-rows">
          <thead>
            <tr>
              <th>Batch ID</th>
              <th>Row #</th>
              <th>Rejection Code</th>
              <th>Reason</th>
              <th>InvoiceNo</th>
              <th>StockCode</th>
              <th>Qty Raw</th>
              <th>Price Raw</th>
              <th>Date Raw</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section id="tab-bi" class="tab-panel">
      <div class="grid-2">
        <div>
          <div class="section-hdr"><span>Top Performing Merchandise SKUs (best_selling_products.sql)</span></div>
          <div class="table-wrap">
            <table id="tbl-products">
              <thead>
                <tr>
                  <th>SKU</th>
                  <th>Description</th>
                  <th>Category</th>
                  <th>Orders</th>
                  <th>Units Sold</th>
                  <th>Net Revenue</th>
                  <th>Return %</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div>
          <div class="section-hdr"><span>Geographic Market Performance (country_performance.sql)</span></div>
          <div class="table-wrap">
            <table id="tbl-countries">
              <thead>
                <tr>
                  <th>Country</th>
                  <th>Customers</th>
                  <th>Orders</th>
                  <th>Gross Rev</th>
                  <th>Returns</th>
                  <th>Net Revenue</th>
                  <th>Cancel %</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="grid-2">
        <div>
          <div class="section-hdr"><span>Customer Order Frequency Cohorts (order_frequency_distribution.sql)</span></div>
          <div class="table-wrap">
            <table id="tbl-freq">
              <thead>
                <tr>
                  <th>Cohort</th>
                  <th>Customers</th>
                  <th>Completed Orders</th>
                  <th>Total Net Revenue</th>
                  <th>Avg LTV</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div>
          <div class="section-hdr"><span>Top Registered Customer Accounts (customer_value_rfm.sql)</span></div>
          <div class="table-wrap">
            <table id="tbl-customers">
              <thead>
                <tr>
                  <th>Customer ID</th>
                  <th>Country</th>
                  <th>Segment</th>
                  <th>Orders</th>
                  <th>Net LTV</th>
                  <th>AOV</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-sql" class="tab-panel">
      <div class="section-hdr">
        <span>Version-Controlled SQL Analytical Models</span>
        <select id="sel-report" onchange="loadSelectedSqlReport()">
          <option value="revenue_trends">revenue_trends.sql</option>
          <option value="best_selling_products">best_selling_products.sql</option>
          <option value="customer_value_rfm">customer_value_rfm.sql</option>
          <option value="order_frequency_distribution">order_frequency_distribution.sql</option>
          <option value="country_performance">country_performance.sql</option>
          <option value="cancellation_and_guest_audit">cancellation_and_guest_audit.sql</option>
        </select>
      </div>
      <pre class="sql-box" id="sql-code-view"></pre>
      <div class="section-hdr"><span>Live Query Execution Output</span></div>
      <div class="table-wrap">
        <table id="tbl-sql-dynamic">
          <thead></thead>
          <tbody></tbody>
        </table>
      </div>
    </section>
  </main>
</div>

<script>
function switchTab(tabKey, btnEl) {
  document.querySelectorAll('.tab-panel').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tabKey).classList.add('active');
  btnEl.classList.add('active');
}

function showToast(msg, isError) {
  const t = document.getElementById('action-toast');
  t.style.display = 'block';
  t.style.background = isError ? 'rgba(239, 68, 68, 0.18)' : 'rgba(16, 185, 129, 0.18)';
  t.style.border = isError ? '1px solid #7f1d1d' : '1px solid #065f46';
  t.style.color = isError ? '#fca5a5' : '#6ee7b7';
  t.textContent = msg;
}

async function refreshAll() {
  const ovRes = await fetch('/api/overview');
  const ov = await ovRes.json();

  document.getElementById('meta-engine').textContent = ov.storage_engine;
  document.getElementById('meta-dag').textContent = ov.dag_id;

  const k = ov.kpis;
  document.getElementById('kpi-net-rev').textContent = '£' + Number(k.net_revenue).toLocaleString(undefined, {minimumFractionDigits:2});
  document.getElementById('kpi-gross-rev').textContent = 'Gross £' + Number(k.gross_revenue).toLocaleString(undefined, {minimumFractionDigits:2});
  document.getElementById('kpi-orders').textContent = Number(k.completed_orders).toLocaleString();
  document.getElementById('kpi-aov').textContent = 'AOV £' + Number(k.avg_order_value).toFixed(2);
  document.getElementById('kpi-ret-rate').textContent = Number(k.return_rate_pct).toFixed(2) + '%';
  document.getElementById('kpi-ret-amt').textContent = '£' + Number(k.return_amount).toLocaleString(undefined, {minimumFractionDigits:2}) + ' across ' + k.cancelled_orders + ' credit notes';
  document.getElementById('kpi-customers').textContent = Number(k.active_customers).toLocaleString();
  document.getElementById('kpi-products').textContent = Number(k.active_products).toLocaleString();

  const latestStatus = ov.runs.length > 0 ? ov.runs[0].status : 'IDLE';
  document.getElementById('top-status-pill').textContent = 'LATEST RUN: ' + latestStatus + ' • ' + Number(k.total_fact_lines).toLocaleString() + ' FACT ROWS';

  const runsBody = document.querySelector('#tbl-runs tbody');
  runsBody.innerHTML = '';
  let failedBannerHtml = '';
  ov.runs.forEach(r => {
    if (r.status === 'FAILED' && !failedBannerHtml) {
      failedBannerHtml = `<div class="alert-banner"><strong>Quality Gate Halted Execution (${r.run_id} | Batch: ${r.batch_id})</strong> &mdash; Stage <code>${r.error_stage}</code>: ${r.error_message}</div>`;
    }
    const badge = r.status === 'SUCCESS' ? '<span class="badge-pass">SUCCESS</span>' : '<span class="badge-fail">FAILED</span>';
    runsBody.innerHTML += `<tr>
      <td>${r.run_id}</td>
      <td>${r.batch_id}</td>
      <td>${r.execution_mode}</td>
      <td>${badge}</td>
      <td>${r.raw_row_count}</td>
      <td>${r.valid_row_count}</td>
      <td>${r.quarantined_row_count}</td>
      <td>${r.duplicate_row_count}</td>
      <td>${r.warehouse_fact_upserts}</td>
      <td>${r.quality_checks_passed}</td>
      <td>${r.quality_checks_failed}</td>
      <td>${Number(r.duration_seconds || 0).toFixed(3)}s</td>
    </tr>`;
  });
  document.getElementById('failed-run-banner').innerHTML = failedBannerHtml;

  const qSumBody = document.querySelector('#tbl-quar-summary tbody');
  qSumBody.innerHTML = '';
  ov.quarantine_summary.forEach(q => {
    qSumBody.innerHTML += `<tr>
      <td>${q.batch_id}</td>
      <td><span class="badge-fail">${q.rejection_code}</span></td>
      <td>${q.rejected_rows}</td>
      <td>${q.sample_reason}</td>
    </tr>`;
  });

  const chkRes = await fetch('/api/quality-checks');
  const chkData = await chkRes.json();
  const chkBody = document.querySelector('#tbl-checks tbody');
  chkBody.innerHTML = '';
  chkData.checks.slice(0, 35).forEach(c => {
    const b = c.passed === 1 ? '<span class="badge-pass">PASS</span>' : '<span class="badge-fail">FAIL</span>';
    chkBody.innerHTML += `<tr>
      <td>${b}</td>
      <td>${c.stage}</td>
      <td>${c.check_name}</td>
      <td>${c.observed_value}</td>
      <td>${c.threshold_expression}</td>
      <td>${c.batch_id}</td>
    </tr>`;
  });

  const qrRes = await fetch('/api/quarantine-records');
  const qrData = await qrRes.json();
  const qrBody = document.querySelector('#tbl-quar-rows tbody');
  qrBody.innerHTML = '';
  qrData.records.slice(0, 40).forEach(r => {
    qrBody.innerHTML += `<tr>
      <td>${r.batch_id}</td>
      <td>${r.source_row_number}</td>
      <td><span class="badge-fail">${r.rejection_code}</span></td>
      <td>${r.rejection_reason}</td>
      <td>${r.invoice_no || '(blank)'}</td>
      <td>${r.stock_code}</td>
      <td>${r.quantity_raw}</td>
      <td>${r.unit_price_raw}</td>
      <td>${r.invoice_date_raw}</td>
    </tr>`;
  });

  await loadBiTables();
  await loadSelectedSqlReport();
}

async function loadBiTables() {
  const [prodRes, countryRes, freqRes, custRes] = await Promise.all([
    fetch('/api/reports/best_selling_products').then(r => r.json()),
    fetch('/api/reports/country_performance').then(r => r.json()),
    fetch('/api/reports/order_frequency_distribution').then(r => r.json()),
    fetch('/api/reports/customer_value_rfm').then(r => r.json())
  ]);

  const prodBody = document.querySelector('#tbl-products tbody');
  prodBody.innerHTML = '';
  prodRes.rows.slice(0, 12).forEach(p => {
    prodBody.innerHTML += `<tr>
      <td>${p.stock_code}</td>
      <td>${p.canonical_description}</td>
      <td>${p.product_category}</td>
      <td>${p.order_count}</td>
      <td>${p.gross_units_sold}</td>
      <td>£${Number(p.net_revenue).toFixed(2)}</td>
      <td>${Number(p.return_rate_pct).toFixed(2)}%</td>
    </tr>`;
  });

  const cBody = document.querySelector('#tbl-countries tbody');
  cBody.innerHTML = '';
  countryRes.rows.forEach(c => {
    cBody.innerHTML += `<tr>
      <td>${c.country}</td>
      <td>${c.identified_customers}</td>
      <td>${c.completed_orders}</td>
      <td>£${Number(c.gross_revenue).toFixed(2)}</td>
      <td>£${Number(c.return_amount).toFixed(2)}</td>
      <td>£${Number(c.net_revenue).toFixed(2)}</td>
      <td>${Number(c.cancellation_rate_pct).toFixed(2)}%</td>
    </tr>`;
  });

  const fBody = document.querySelector('#tbl-freq tbody');
  fBody.innerHTML = '';
  freqRes.rows.forEach(f => {
    fBody.innerHTML += `<tr>
      <td>${f.frequency_bucket}</td>
      <td>${f.customer_count}</td>
      <td>${f.total_completed_orders}</td>
      <td>£${Number(f.total_net_revenue).toFixed(2)}</td>
      <td>£${Number(f.avg_customer_ltv).toFixed(2)}</td>
    </tr>`;
  });

  const custBody = document.querySelector('#tbl-customers tbody');
  custBody.innerHTML = '';
  custRes.rows.filter(c => c.is_guest === 0).slice(0, 12).forEach(c => {
    custBody.innerHTML += `<tr>
      <td>${c.customer_id}</td>
      <td>${c.primary_country}</td>
      <td>${c.customer_segment}</td>
      <td>${c.lifetime_orders}</td>
      <td>£${Number(c.lifetime_net_revenue).toFixed(2)}</td>
      <td>£${Number(c.avg_order_value).toFixed(2)}</td>
    </tr>`;
  });
}

async function loadSelectedSqlReport() {
  const name = document.getElementById('sel-report').value;
  const res = await fetch('/api/reports/' + name);
  const data = await res.json();
  document.getElementById('sql-code-view').textContent = data.sql;

  const thead = document.querySelector('#tbl-sql-dynamic thead');
  const tbody = document.querySelector('#tbl-sql-dynamic tbody');
  thead.innerHTML = '';
  tbody.innerHTML = '';
  if (data.rows.length === 0) return;

  const cols = Object.keys(data.rows[0]);
  thead.innerHTML = '<tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr>';
  data.rows.slice(0, 30).forEach(row => {
    tbody.innerHTML += '<tr>' + cols.map(c => `<td>${row[c]}</td>`).join('') + '</tr>';
  });
}

async function triggerSyntheticBatch() {
  const batch_id = document.getElementById('inp-batch-id').value;
  const invoices = parseInt(document.getElementById('inp-invoices').value || '180', 10);
  const seed = parseInt(document.getElementById('inp-seed').value || '204', 10);
  showToast('Running ETL pipeline for ' + batch_id + '...', false);
  const res = await fetch('/api/trigger/synthetic', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({batch_id, start_date: '2026-10-15', days: 30, invoices, seed})
  });
  const out = await res.json();
  if (out.status === 'SUCCESS') {
    showToast(`Batch ${out.batch_id} succeeded: ${out.valid_row_count} valid rows loaded.`, false);
  } else {
    showToast(`Batch ${out.batch_id} halted: ${out.error_message}`, true);
  }
  await refreshAll();
}

async function triggerCorruptBatch() {
  showToast('Injecting corrupt batch to test quality gate...', false);
  const res = await fetch('/api/trigger/corrupt', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({batch_id: '2026_q4_corrupt_feed'})
  });
  const out = await res.json();
  showToast(`Gate halted corrupt batch at ${out.error_stage} (${out.status}). Warehouse protected.`, true);
  await refreshAll();
}

window.addEventListener('DOMContentLoaded', refreshAll);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def serve_workbench_ui():
    return WORKBENCH_HTML
