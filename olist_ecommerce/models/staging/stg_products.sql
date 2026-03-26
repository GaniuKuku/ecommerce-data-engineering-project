WITH raw_products AS (
    SELECT * FROM {{ source('ecommerce_dw', 'products') }}
),

raw_translations AS (
    -- Renaming BigQuery's auto-generated columns back to their proper names
    SELECT 
        string_field_0 AS product_category_name,
        string_field_1 AS product_category_name_english
    FROM {{ source('ecommerce_dw', 'category_translation') }}
    -- This filters out the actual CSV header row so it isn't treated as data
    WHERE string_field_0 != 'product_category_name' 
)

SELECT
    p.product_id,
    p.product_category_name AS category_name_portuguese,
    COALESCE(t.product_category_name_english, 'unknown') AS category_name_english,
    
    p.product_name_lenght AS product_name_length,
    p.product_description_lenght AS product_description_length,
    
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

FROM raw_products p
LEFT JOIN raw_translations t
    ON p.product_category_name = t.product_category_name