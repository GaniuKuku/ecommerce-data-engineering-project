WITH order_payments AS (
    SELECT 
        order_id,
        payment_sequential,
        payment_value
    FROM {{ ref('stg_order_payments') }}
)

SELECT
    order_id,
    COUNT(payment_sequential) AS total_payment_methods_used,
    SUM(payment_value) AS total_payment_value

FROM order_payments
GROUP BY order_id