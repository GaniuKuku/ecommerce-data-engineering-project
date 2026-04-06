WITH order_items AS (
    SELECT 
        order_id,
        order_item_id,
        price,
        freight_value
    FROM {{ ref('stg_order_items') }}
)

SELECT
    order_id,
    COUNT(order_item_id) AS total_items_in_order,
    SUM(price) AS total_item_value,
    SUM(freight_value) AS total_freight_value

FROM order_items
GROUP BY order_id