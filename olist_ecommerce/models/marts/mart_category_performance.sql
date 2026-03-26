WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

-- We need this "bridge" to get the product_id for each order
items AS (
    SELECT * FROM {{ source('ecommerce_dw', 'order_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
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