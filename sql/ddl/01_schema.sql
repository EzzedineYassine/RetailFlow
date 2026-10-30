CREATE TABLE IF NOT EXISTS ops_pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    dag_id VARCHAR(128) NOT NULL,
    execution_mode VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at VARCHAR(40) NOT NULL,
    finished_at VARCHAR(40),
    duration_seconds REAL,
    source_file VARCHAR(512),
    raw_row_count INTEGER DEFAULT 0,
    valid_row_count INTEGER DEFAULT 0,
    quarantined_row_count INTEGER DEFAULT 0,
    duplicate_row_count INTEGER DEFAULT 0,
    warehouse_fact_upserts INTEGER DEFAULT 0,
    quality_checks_total INTEGER DEFAULT 0,
    quality_checks_passed INTEGER DEFAULT 0,
    quality_checks_failed INTEGER DEFAULT 0,
    error_stage VARCHAR(64),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ops_quality_checks (
    check_id VARCHAR(96) PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    stage VARCHAR(32) NOT NULL,
    check_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    passed INTEGER NOT NULL,
    observed_value REAL NOT NULL,
    threshold_expression VARCHAR(128) NOT NULL,
    details TEXT NOT NULL,
    evaluated_at VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS ops_quarantine_records (
    quarantine_id VARCHAR(96) PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    source_row_number INTEGER NOT NULL,
    invoice_no VARCHAR(64),
    stock_code VARCHAR(64),
    description VARCHAR(255),
    quantity_raw VARCHAR(64),
    invoice_date_raw VARCHAR(64),
    unit_price_raw VARCHAR(64),
    customer_id_raw VARCHAR(64),
    country_raw VARCHAR(128),
    rejection_code VARCHAR(64) NOT NULL,
    rejection_reason TEXT NOT NULL,
    quarantined_at VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_transactions (
    raw_row_id VARCHAR(96) PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    source_file VARCHAR(512) NOT NULL,
    source_row_number INTEGER NOT NULL,
    row_hash VARCHAR(64) NOT NULL,
    invoice_no VARCHAR(64),
    stock_code VARCHAR(64),
    description VARCHAR(255),
    quantity VARCHAR(64),
    invoice_date VARCHAR(64),
    unit_price VARCHAR(64),
    customer_id VARCHAR(64),
    country VARCHAR(128),
    ingested_at VARCHAR(40) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_transactions_batch ON raw_transactions(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_hash ON raw_transactions(row_hash);

CREATE TABLE IF NOT EXISTS stg_transactions (
    stg_line_id VARCHAR(96) PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    invoice_no VARCHAR(64) NOT NULL,
    stock_code VARCHAR(64) NOT NULL,
    description VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    invoice_timestamp VARCHAR(40) NOT NULL,
    invoice_date VARCHAR(10) NOT NULL,
    date_key INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    gross_amount REAL NOT NULL,
    return_amount REAL NOT NULL,
    net_amount REAL NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    is_guest_customer INTEGER NOT NULL,
    country VARCHAR(128) NOT NULL,
    transaction_type VARCHAR(32) NOT NULL,
    is_cancelled INTEGER NOT NULL,
    is_return INTEGER NOT NULL,
    occurrence_count INTEGER NOT NULL,
    cleaned_at VARCHAR(40) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_transactions_batch ON stg_transactions(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_transactions_date_key ON stg_transactions(date_key);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date VARCHAR(10) NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(16) NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(16) NOT NULL,
    is_weekend INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    is_guest INTEGER NOT NULL,
    primary_country VARCHAR(128) NOT NULL,
    first_purchase_date VARCHAR(10) NOT NULL,
    last_purchase_date VARCHAR(10) NOT NULL,
    lifetime_orders INTEGER NOT NULL,
    lifetime_cancelled_orders INTEGER NOT NULL,
    lifetime_gross_revenue REAL NOT NULL,
    lifetime_return_amount REAL NOT NULL,
    lifetime_net_revenue REAL NOT NULL,
    customer_segment VARCHAR(32) NOT NULL,
    updated_at VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key VARCHAR(64) PRIMARY KEY,
    stock_code VARCHAR(64) NOT NULL UNIQUE,
    canonical_description VARCHAR(255) NOT NULL,
    product_category VARCHAR(64) NOT NULL,
    avg_unit_price REAL NOT NULL,
    min_unit_price REAL NOT NULL,
    max_unit_price REAL NOT NULL,
    total_units_sold INTEGER NOT NULL,
    total_units_returned INTEGER NOT NULL,
    net_revenue REAL NOT NULL,
    first_sold_date VARCHAR(10) NOT NULL,
    last_sold_date VARCHAR(10) NOT NULL,
    updated_at VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_fact_id VARCHAR(96) PRIMARY KEY,
    invoice_no VARCHAR(64) NOT NULL,
    product_key VARCHAR(64) NOT NULL,
    customer_key VARCHAR(64) NOT NULL,
    date_key INTEGER NOT NULL,
    invoice_timestamp VARCHAR(40) NOT NULL,
    country VARCHAR(128) NOT NULL,
    transaction_type VARCHAR(32) NOT NULL,
    is_cancelled INTEGER NOT NULL,
    is_return INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    gross_amount REAL NOT NULL,
    return_amount REAL NOT NULL,
    net_amount REAL NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    last_run_id VARCHAR(64) NOT NULL,
    loaded_at VARCHAR(40) NOT NULL,
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice_no ON fact_sales(invoice_no);
CREATE INDEX IF NOT EXISTS idx_fact_sales_batch_id ON fact_sales(batch_id);
