{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','product_category_name_translation') }}
)

SELECT
    raw.product_category_name          AS product_category_name,
    raw.product_category_name_english  AS product_category_name_english
FROM raw
