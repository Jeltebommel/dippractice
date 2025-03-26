{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','customers') }}
)
SELECT
CAST(raw.customer_id AS BIGINT) AS customer_id,
CAST(raw.customer_unique_id AS BIGINT) AS customer_unique_id,
raw.customer_zip_code_prefix AS customer_zip_code,
raw.customer_city as customer_city,
raw.customer_state as customer_state
FROM
raw












