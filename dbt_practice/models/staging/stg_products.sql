{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','products') }}
)

SELECT
    CAST(raw.product_id AS BIGINT)                AS product_id,
    raw.product_category_name     AS product_category_name,
    CAST(raw.product_name_lenght AS BIGINT)          AS product_name_lenght,
    CAST(raw.product_description_lenght AS BIGINT)   AS product_description_lenght,
    CAST(raw.product_photos_qty AS BIGINT)           AS product_photos_qty,
    CAST(raw.product_weight_g AS BIGINT)             AS product_weight_g,
    CAST(raw.product_length_cm AS BIGINT)            AS product_length_cm,
    CAST(raw.product_height_cm AS BIGINT)            AS product_height_cm,
    CAST(raw.product_width_cm AS BIGINT)             AS product_width_cm

FROM raw
