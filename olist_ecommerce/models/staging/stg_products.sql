WITH raw_products AS (
    SELECT 
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM {{ source('ecommerce_dw', 'products') }}
),

-- 1. Cleans the Products BEFORE the join
clean_products AS (
    SELECT * FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY product_id) as row_num
        FROM raw_products
    )
    WHERE row_num = 1
),

-- 2. Cleans the Translations BEFORE the join
clean_translations AS (
    SELECT * FROM (
        SELECT 
            string_field_0 AS product_category_name,
            string_field_1 AS product_category_name_english,
            ROW_NUMBER() OVER(PARTITION BY string_field_0 ORDER BY string_field_1) as row_num
        FROM {{ source('ecommerce_dw', 'category_translation') }}
        WHERE string_field_0 != 'product_category_name' 
    )
    WHERE row_num = 1
)

-- 3. Safely join the two clean tables together
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
FROM clean_products p
LEFT JOIN clean_translations t
    ON p.product_category_name = t.product_category_name