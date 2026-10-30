SELECT
    CASE
        WHEN c.lifetime_orders = 1 THEN '1 Order (One-Time)'
        WHEN c.lifetime_orders BETWEEN 2 AND 3 THEN '2-3 Orders (Repeat)'
        WHEN c.lifetime_orders BETWEEN 4 AND 7 THEN '4-7 Orders (Loyal)'
        ELSE '8+ Orders (VIP Power Buyer)'
    END AS frequency_bucket,
    COUNT(*) AS customer_count,
    SUM(c.lifetime_orders) AS total_completed_orders,
    ROUND(SUM(c.lifetime_net_revenue), 2) AS total_net_revenue,
    ROUND(AVG(c.lifetime_net_revenue), 2) AS avg_customer_ltv
FROM dim_customer c
WHERE c.is_guest = 0 AND c.lifetime_orders > 0
GROUP BY
    CASE
        WHEN c.lifetime_orders = 1 THEN '1 Order (One-Time)'
        WHEN c.lifetime_orders BETWEEN 2 AND 3 THEN '2-3 Orders (Repeat)'
        WHEN c.lifetime_orders BETWEEN 4 AND 7 THEN '4-7 Orders (Loyal)'
        ELSE '8+ Orders (VIP Power Buyer)'
    END
ORDER BY MIN(c.lifetime_orders) ASC;
