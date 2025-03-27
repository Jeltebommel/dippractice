{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','payments') }}
)

SELECT
    CAST(raw.order_id AS BIGINT)           AS order_id,
    CAST(raw.payment_sequential AS BIGINT) AS payment_sequential,
    raw.payment_type                       AS payment_type,
    CAST(raw.payment_installments AS BIGINT) AS payment_installments,
    CAST(raw.payment_value AS DOUBLE)      AS payment_value

FROM raw


-- vraag aan Stefan: wat voor datatypes zijn best practice voor prijzen? Ik gebruik zelf altijd een float

