WITH raw_customers AS (
    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM {{ source('ecommerce_dw', 'customers') }}
)

SELECT
    customer_id,
    customer_unique_id,
    
    -- Renaming these to be shorter and cleaner.
    customer_zip_code_prefix AS zip_code,
    customer_city AS city,
    customer_state AS state

FROM raw_customers