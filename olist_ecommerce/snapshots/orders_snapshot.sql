{% snapshot orders_snapshot %}

{{
    config(
      target_schema='ecommerce_snapshots', 
      unique_key='order_id',
      strategy='check',
      check_cols=['order_status']
    )
}}

-- Point this at your raw orders table (adjust the name if your raw dataset is named differently)
select * from {{ source('ecommerce_dw', 'orders') }}

{% endsnapshot %}