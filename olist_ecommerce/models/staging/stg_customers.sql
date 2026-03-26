WITH raw_customers AS (
    SELECT * FROM {{ source('ecommerce_dw', 'customers') }}
)

SELECT
    customer_id,
    customer_unique_id,
    
    -- Renaming these to be shorter and cleaner for the BI tool
    customer_zip_code_prefix AS zip_code,
    customer_city AS city,
    customer_state AS state

FROM raw_customers