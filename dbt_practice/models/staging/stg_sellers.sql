{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','sellers') }}
)

SELECT
    CAST(raw.seller_id AS BIGINT)    AS seller_id,
    CAST(raw.seller_zip_code_prefix AS BIGINT)  AS seller_zip_code_prefix,
    raw.seller_city                         AS seller_city,
    raw.seller_state                        AS seller_state

FROM raw
