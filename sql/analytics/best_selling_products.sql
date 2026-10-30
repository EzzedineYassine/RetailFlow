SELECT
    p.stock_code,
    p.canonical_description,
    p.product_category,
    COUNT(DISTINCT CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.invoice_no END) AS order_count,
    SUM(CASE WHEN f.is_cancelled = 0 AND f.is_return = 0 THEN f.quantity ELSE 0 END) AS gross_units_sold,
    SUM(CASE WHEN f.is_cancelled = 1 OR f.is_return = 1 THEN ABS(f.quantity) ELSE 0 END) AS units_returned,
    ROUND(AVG(f.unit_price), 2) AS avg_selling_price,
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
    ) AS return_rate_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.stock_code, p.canonical_description, p.product_category
ORDER BY net_revenue DESC;
