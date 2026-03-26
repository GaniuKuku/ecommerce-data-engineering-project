WITH order_payments AS (
    SELECT * FROM {{ source('ecommerce_dw', 'payments') }}
)

SELECT
    order_id,
    COUNT(payment_sequential) AS total_payment_methods_used,
    SUM(payment_value) AS total_payment_value

FROM order_payments
GROUP BY order_id