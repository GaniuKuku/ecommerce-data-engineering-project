SELECT
    order_id,
    order_status,
    -- Actual time taken to deliver
    TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS days_to_deliver,
    -- Difference between estimated and actual (Positive = Early, Negative = Late)
    TIMESTAMP_DIFF(order_estimated_delivery_date, order_delivered_customer_date, DAY) AS delivery_accuracy_days,
    -- Flagging late orders
    CASE 
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 'Late'
        ELSE 'On-Time/Early'
    END AS delivery_status
FROM {{ ref('fct_orders') }}
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL