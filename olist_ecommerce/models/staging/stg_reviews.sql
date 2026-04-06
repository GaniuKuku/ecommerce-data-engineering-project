WITH raw_reviews AS (
    SELECT 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM {{ source('ecommerce_dw', 'reviews') }}
)

SELECT
    review_id,
    order_id,
    review_score,
    
    -- Replacing empty text with a clean placeholder
    COALESCE(review_comment_title, 'No Title') AS review_title,
    COALESCE(review_comment_message, 'No Comment') AS review_message,
    
    -- Casting strings to proper Timestamps
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp

FROM raw_reviews