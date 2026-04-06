WITH raw_sellers AS (
    SELECT 
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM {{ source('ecommerce_dw', 'sellers') }}
)

SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    seller_city AS city,
    seller_state AS state

FROM raw_sellers