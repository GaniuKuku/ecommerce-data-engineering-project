with source as (
    -- Pulling from the raw BigQuery table created by your Python script
    select * from {{ source('ecommerce_dw', 'payments') }}
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value -- Total amount the customer actually paid
    from source
)

select * from renamed