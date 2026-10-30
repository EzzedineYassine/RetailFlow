SELECT
    d.full_date AS transaction_date,
    d.year_month,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.invoice_no END) AS completed_orders,
    COUNT(DISTINCT CASE WHEN f.is_cancelled = 1 OR f.is_return = 1 THEN f.invoice_no END) AS cancelled_or_returned_orders,
    SUM(CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.quantity ELSE 0 END) AS units_sold,
    SUM(CASE WHEN f.is_cancelled = 1 OR f.is_return = 1 THEN ABS(f.quantity) ELSE 0 END) AS units_returned,
    ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
    ROUND(SUM(f.return_amount), 2) AS return_value,
    ROUND(SUM(f.net_amount), 2) AS net_revenue,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.invoice_no END) > 0
            THEN SUM(f.net_amount) / COUNT(DISTINCT CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.invoice_no END)
            ELSE 0.0
        END,
        2
    ) AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year_month, d.day_name, d.is_weekend
ORDER BY d.full_date ASC;
