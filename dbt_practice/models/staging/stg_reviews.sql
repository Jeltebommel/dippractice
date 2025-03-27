{{ config(
    materialized='table',
    schema='jelte_staging'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('jelte_raw','reviews') }}
)

SELECT
    CAST(raw.review_id AS BIGINT) AS review_id,
    CAST(raw.order_id AS BIGINT)  AS order_id,
    CAST(raw.review_score AS BIGINT)    AS review_score,
    raw.review_comment_title      AS review_comment_title,
    raw.review_comment_message    AS review_comment_message,
    CAST(raw.review_creation_date AS TIMESTAMP)  AS review_creation_date,
    CAST(raw.review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp

FROM raw
