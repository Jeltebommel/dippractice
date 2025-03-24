{{ config(
    materialized='table',
    schema='jelte_stg_calls'
) }}

WITH raw AS (
    SELECT
        *
    FROM {{ source('jelte_raw', 'medewerkerslijst') }}
)

SELECT
    raw.`Intern_ID`       AS intern_id,
    raw.`Achternaam`      AS last_name,
    raw.`Achtervoegsel`   AS suffix,
    raw.`Actief`          AS actief,
    raw.`Year`            AS year,
    raw.`Quarter`         AS quarter,
    raw.`Month`           AS month,
    raw.`Day`             AS day_of_month,
    raw.`BPMS_User`       AS bpms_user,
    raw.`Dagen_in_dienst` AS days_in_service,
    raw.`Dienstweek`      AS dienstweek,
    raw.`E-mail`          AS email,         -- MUST be quoted because of the dash
    raw.`Groep`           AS groep,
    raw.`Filter`          AS filter_val,
    raw.`Is_supervisor`   AS is_supervisor
FROM raw

