WITH orders AS (
    SELECT 
        order_id, 
        total_payment_value
    FROM {{ ref('fct_orders') }}
),

-- Use the Staging model here, NEVER the raw source!
items AS (
    SELECT 
        order_id, 
        product_id, 
        price
    FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT 
        product_id, 
        category_name_english
    FROM {{ ref('stg_products') }}
)

SELECT
    p.category_name_english,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_payment_value) AS total_sales_value,
    AVG(i.price) AS avg_item_price
FROM orders o
JOIN items i ON o.order_id = i.order_id
JOIN products p ON i.product_id = p.product_id
GROUP BY 1
ORDER BY total_sales_value DESC