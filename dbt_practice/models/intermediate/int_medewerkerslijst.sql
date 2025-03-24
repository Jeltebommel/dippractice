{{ config(
    materialized='table',
    schema='jelte_int_calls'
) }}

WITH medewerkers_stg AS (
    SELECT
        intern_id,
        last_name,
        suffix,
        actief,
        year,
        quarter,
        month,
        day_of_month,
        bpms_user,
        days_in_service,
        dienstweek,
        email,
        groep,
        filter_val,
        is_supervisor
    FROM {{ ref('stg_medewerkerslijst') }}  -- reference the STAGING model
),

transformed AS (
    SELECT
        intern_id,
        
        -- Example: Combine last_name + suffix
        CASE 
            WHEN suffix IS NOT NULL AND suffix != ''
            THEN CONCAT(last_name, ' ', suffix)
            ELSE last_name
        END AS full_name,

        -- Convert actief: e.g., "Ja" -> TRUE, "Nee" -> FALSE
        CASE LOWER(actief)
            WHEN 'ja' THEN TRUE
            WHEN 'nee' THEN FALSE
            ELSE NULL
        END AS is_active,

        CAST(year AS INT) AS year,
        quarter,
        month,
        day_of_month,
        bpms_user,
        days_in_service,
        dienstweek,
        email,
        groep,
        filter_val,
        is_supervisor,

        -- Optionally, create a single date from year-month-day
        TO_DATE(CONCAT(year,'-', LPAD(month,2,'0'), '-', LPAD(day_of_month,2,'0'))) AS hire_date,

        CURRENT_TIMESTAMP AS load_timestamp
    FROM medewerkers_stg
)

SELECT *
FROM transformed
