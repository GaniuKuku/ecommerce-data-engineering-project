-- mart_kpis.sql
-- This model provides high-level business health metrics in a single row
-- for use in dashboard headers/scorecards.

SELECT
    SUM(f.total_payment_value) AS lifetime_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_payment_value) / COUNT(DISTINCT f.order_id) AS avg_order_value,
    
    -- Here is the fix: Counting the TRUE unique human ID
    COUNT(DISTINCT c.customer_unique_id) AS total_unique_customers,
    
    SUM(f.total_items) / COUNT(DISTINCT f.order_id) AS avg_items_per_order

FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('stg_customers') }} c 
    ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'