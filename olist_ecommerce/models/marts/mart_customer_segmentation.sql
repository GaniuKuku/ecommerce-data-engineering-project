SELECT
    c.customer_unique_id,
    c.city,
    c.state,
    COUNT(f.order_id) AS lifetime_orders,
    SUM(f.total_payment_value) AS lifetime_value_ltv,
    MAX(f.order_purchase_timestamp) AS last_purchase_date
FROM {{ ref('fct_orders') }} f
JOIN {{ ref('stg_customers') }} c ON f.customer_id = c.customer_id
GROUP BY 1, 2, 3
ORDER BY lifetime_value_ltv DESC