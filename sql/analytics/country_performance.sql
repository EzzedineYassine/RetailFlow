SELECT
    f.country,
    COUNT(DISTINCT CASE WHEN c.is_guest = 0 THEN f.customer_key END) AS identified_customers,
    COUNT(DISTINCT CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.invoice_no END) AS completed_orders,
    COUNT(DISTINCT CASE WHEN f.is_cancelled = 1 OR f.is_return = 1 THEN f.invoice_no END) AS cancelled_orders,
    SUM(CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.quantity ELSE 0 END) AS net_units_sold,
    ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
    ROUND(SUM(f.return_amount), 2) AS return_amount,
    ROUND(SUM(f.net_amount), 2) AS net_revenue,
    ROUND(
        CASE
            WHEN SUM(f.gross_amount) > 0
            THEN (SUM(f.return_amount) / SUM(f.gross_amount)) * 100.0
            ELSE 0.0
        END,
        2
    ) AS cancellation_rate_pct
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY f.country
ORDER BY net_revenue DESC;
