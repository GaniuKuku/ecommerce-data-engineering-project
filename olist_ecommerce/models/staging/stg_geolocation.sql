WITH raw_geo AS (
    SELECT * FROM {{ source('ecommerce_dw', 'geolocation') }}
)

SELECT
    geolocation_zip_code_prefix AS zip_code,
    
    -- Averaging the coordinates to get exactly ONE point per zip code
    AVG(geolocation_lat) AS lat,
    AVG(geolocation_lng) AS lng,
    
    MAX(geolocation_city) AS city,
    MAX(geolocation_state) AS state

FROM raw_geo
GROUP BY geolocation_zip_code_prefix