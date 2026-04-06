-- tests/assert_delivery_is_after_purchase.sql

-- This test finds any orders where the delivery date happened BEFORE the purchase date.
-- If this query returns ANY rows, the pipeline will fail.

select 
    order_id, 
    order_purchase_timestamp, 
    order_delivered_customer_date
from {{ ref('fct_orders') }}
where order_delivered_customer_date < order_purchase_timestamp