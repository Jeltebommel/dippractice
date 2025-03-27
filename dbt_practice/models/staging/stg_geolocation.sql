{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','geolocation') }}
)
SELECT
raw.geolocation_zip_code_prefix AS geo_zip_code,
raw.geolocation_lat AS geo_latitude,
raw.geolocation_lng AS geo_longitude,
raw.geolocation_city AS geo_city,
raw.geolocation_state AS geo_state
FROM
raw
