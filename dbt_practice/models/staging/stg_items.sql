{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','items') }}
)
SELECT
CAST(raw.order_id AS BIGINT) AS order_id,
raw.order_item_id AS order_items_id,
CAST(raw.product_id AS BIGINT) AS product_id,
CAST(raw.seller_id AS BIGINT) AS seller_id,
raw.shipping_limit_date AS shipping_limit_date,
CAST(raw.price AS FLOAT) AS price,
CAST(raw.freight_value AS FLOAT) AS freight_value
FROM raw
