SELECT
    c.customer_id,
    c.is_guest,
    c.primary_country,
    c.customer_segment,
    c.first_purchase_date,
    c.last_purchase_date,
    c.lifetime_orders,
    c.lifetime_cancelled_orders,
    ROUND(c.lifetime_gross_revenue, 2) AS lifetime_gross_revenue,
    ROUND(c.lifetime_return_amount, 2) AS lifetime_return_amount,
    ROUND(c.lifetime_net_revenue, 2) AS lifetime_net_revenue,
    ROUND(
        CASE
            WHEN c.lifetime_orders > 0
            THEN c.lifetime_net_revenue / c.lifetime_orders
            ELSE 0.0
        END,
        2
    ) AS avg_order_value
FROM dim_customer c
ORDER BY c.lifetime_net_revenue DESC;
