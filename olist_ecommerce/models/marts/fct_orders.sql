{{
    config(
        materialized='incremental',
        unique_key='order_id',
        partition_by={
          "field": "order_purchase_timestamp",
          "data_type": "timestamp",
          "granularity": "month"
        },
        cluster_by = "customer_id"
    )
}}

WITH orders AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM {{ ref('stg_orders') }}
    
    {% if is_incremental() %}
        -- Only grab orders that happened AFTER the latest order currently in this table
        WHERE order_purchase_timestamp > (SELECT MAX(order_purchase_timestamp) FROM {{ this }})
    {% endif %}
),

order_items AS (
    SELECT 
        order_id, 
        total_items_in_order, 
        total_item_value, 
        total_freight_value
    FROM {{ ref('int_order_items_agg') }}
),

order_payments AS (
    SELECT 
        order_id, 
        total_payment_methods_used, 
        total_payment_value
    FROM {{ ref('int_order_payments_agg') }}
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
    
    -- Item Metrics
    COALESCE(i.total_items_in_order, 0) AS total_items,
    COALESCE(i.total_item_value, 0) AS total_item_value,
    COALESCE(i.total_freight_value, 0) AS total_freight_value,
    
    -- Payment Metrics
    COALESCE(p.total_payment_methods_used, 0) AS total_payment_methods,
    COALESCE(p.total_payment_value, 0) AS total_payment_value

FROM orders o
LEFT JOIN order_items i ON o.order_id = i.order_id
LEFT JOIN order_payments p ON o.order_id = p.order_id