WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('int_order_items_agg') }}
),

order_payments AS (
    SELECT * FROM {{ ref('int_order_payments_agg') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    
    -- Timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    -- Item Metrics (Replacing nulls with 0 in case an order had no items recorded)
    COALESCE(i.total_items_in_order, 0) AS total_items,
    COALESCE(i.total_item_value, 0) AS total_item_value,
    COALESCE(i.total_freight_value, 0) AS total_freight_value,
    
    -- Payment Metrics
    COALESCE(p.total_payment_methods_used, 0) AS total_payment_methods,
    COALESCE(p.total_payment_value, 0) AS total_payment_value

FROM orders o
LEFT JOIN order_items i ON o.order_id = i.order_id
LEFT JOIN order_payments p ON o.order_id = p.order_id