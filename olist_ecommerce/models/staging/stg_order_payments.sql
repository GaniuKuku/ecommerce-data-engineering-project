with source as (
    -- Pulling strictly what i need from the raw BigQuery table
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value -- Total amount the customer actually paid
    from {{ source('ecommerce_dw', 'payments') }}
)

select * from source