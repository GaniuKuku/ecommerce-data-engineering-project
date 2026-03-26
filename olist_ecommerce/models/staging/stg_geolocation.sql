{{
    config(
        materialized='incremental',
        unique_key='zip_code'
    )
}}

WITH raw_geo AS (
    SELECT * FROM {{ source('ecommerce_dw', 'geolocation') }}
    
    {% if is_incremental() %}
        -- This only runs on the 2nd, 3rd, 4th runs...
        -- It tells dbt: "Only grab zip codes I haven't processed yet"
        WHERE geolocation_zip_code_prefix NOT IN (SELECT zip_code FROM {{ this }})
    {% endif %}
)

SELECT
    geolocation_zip_code_prefix AS zip_code,
    
    -- Averaging the coordinates to get exactly ONE point per zip code
    AVG(geolocation_lat) AS lat,
    AVG(geolocation_lng) AS lng,
    
    -- Using MAX to pick one representative city/state name per zip
    MAX(geolocation_city) AS city,
    MAX(geolocation_state) AS state

FROM raw_geo
GROUP BY 1