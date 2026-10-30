SELECT
    d.year_month,
    COUNT(*) AS total_line_items,
    SUM(CASE WHEN f.transaction_type = 'SALE' THEN 1 ELSE 0 END) AS sale_lines,
    SUM(CASE WHEN f.transaction_type = 'CANCELLATION' THEN 1 ELSE 0 END) AS cancellation_lines,
    SUM(CASE WHEN f.transaction_type = 'RETURN_ADJUSTMENT' THEN 1 ELSE 0 END) AS adjustment_lines,
    SUM(CASE WHEN c.is_guest = 1 THEN 1 ELSE 0 END) AS guest_checkout_lines,
    ROUND(SUM(CASE WHEN c.is_guest = 1 THEN f.net_amount ELSE 0 END), 2) AS guest_net_revenue,
    ROUND(SUM(CASE WHEN c.is_guest = 0 THEN f.net_amount ELSE 0 END), 2) AS registered_net_revenue,
    ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
    ROUND(SUM(f.return_amount), 2) AS return_leakage,
    ROUND(SUM(f.net_amount), 2) AS net_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY d.year_month
ORDER BY d.year_month ASC;
