-- mart_kpis.sql
-- This model provides high-level business health metrics in a single row
-- for use in dashboard headers/scorecards.

SELECT
    SUM(total_payment_value) AS lifetime_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_payment_value) / COUNT(DISTINCT order_id) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS total_unique_customers,
    
    -- Calculating the average items per order
    SUM(total_items) / COUNT(DISTINCT order_id) AS avg_items_per_order

FROM {{ ref('fct_orders') }}
WHERE order_status = 'delivered'