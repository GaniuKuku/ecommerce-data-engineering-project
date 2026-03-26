SELECT
    DATE_TRUNC(order_purchase_timestamp, MONTH) AS order_month,
    COUNT(order_id) AS total_orders,
    SUM(total_payment_value) AS total_revenue,
    -- Average Order Value (AOV)
    SUM(total_payment_value) / NULLIF(COUNT(order_id), 0) AS avg_order_value
FROM {{ ref('fct_orders') }}
WHERE order_status != 'canceled'
GROUP BY 1
ORDER BY 1 DESC