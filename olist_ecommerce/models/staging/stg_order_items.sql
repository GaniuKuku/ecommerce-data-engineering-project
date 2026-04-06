with source as (
    -- Pulling strictly what we need from the raw BigQuery table
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value -- Shipping cost
    from {{ source('ecommerce_dw', 'order_items') }}
)

select * from source