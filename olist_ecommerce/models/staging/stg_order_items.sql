with source as (
    -- Pulling from the raw BigQuery table created by your Python script
    select * from {{ source('ecommerce_dw', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value -- Shipping cost
    from source
)

select * from renamed