{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','orders') }}
)
SELECT
CAST(raw.order_id AS BIGINT) AS order_id,
CAST(raw.customer_id AS BIGINT) AS customer_id,
CAST(raw.order_status AS BIGINT) AS order_status,
raw.order_purchase_timestamp AS order_purchase_timestamp,
raw.order_approved_at AS order_approved_at,
raw.order_delivered_carrier_date AS order_delivered_carrier_date,
raw.order_delivered_customer_date AS order_delivered_customer_date,
raw.order_estimated_delivery_date AS order_estimated_delivery_date
FROM raw
