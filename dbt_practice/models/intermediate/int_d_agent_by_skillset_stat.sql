{{ config(
    materialized='table',
    schema='jelte_int_calls'
) }}

WITH agent_calls AS (
    SELECT
        intern_id,
        agent_given_name,
        agent_surname,
        blended_active_time,
        call_only,        
        call_type,
        calls_answered,
        calls_conferenced,
        calls_offered,
        calls_return_to_q,
        calls_return_to_q_due_to_timeout,
        calls_transferred,
        consult_time,
        contact_hold_time,
        contact_talk_time,
        contact_type,
        dn_out_ext_talk_time,
        dn_out_int_talk_time,
        hold_time,
        idle_time,
        last_name,
        max_capacity_idle_time,
        max_capacity_total_staffed_time,
        not_ready_time,
        post_call_processing_time,
        ring_time,
        short_calls_answered,
        site,
        site_id,
        skillset,
        skillset_id,
        sub_idle_time_voice,
        sub_idle_time_wc,
        talk_time,
        time_event,       -- if the staging column is literally named `Time`
        event_timestamp, -- if the staging column is literally named `Timestamp`
        total_staffed_time,
        user_id,
        werkdag,
        wait_time,
        werkweek
    FROM {{ ref('stg_d_agent_by_skillset_stat') }}  -- reference the STAGING model
),

transformed AS (
    SELECT
        intern_id,
        
        -- Example transformation: combine first and last names
        CONCAT(agent_given_name, ' ', agent_surname) AS agent_full_name,

        -- Convert certain columns to integer if they are strings
        CAST(blended_active_time AS INT) AS blended_active_time_seconds,

        call_only,
        call_type,
        calls_answered,
        calls_conferenced,
        calls_offered,
        calls_return_to_q,
        calls_return_to_q_due_to_timeout,
        calls_transferred,

        -- Potential renaming for clarity
        consult_time                         AS consult_time_seconds,
        contact_hold_time                    AS contact_hold_seconds,
        contact_talk_time                    AS contact_talk_seconds,
        talk_time                            AS total_talk_time_seconds,

        -- Keep the rest as is, or rename if it improves clarity
        skillset,
        skillset_id,
        not_ready_time,
        ring_time,
        short_calls_answered,
        site,
        site_id,
        
        -- If you want to unify date/time
        event_timestamp,
        time_event,
        werkdag,
        werkweek,
        
        wait_time,
        total_staffed_time,
        
        user_id,
        
        -- For auditing or lineage
        CURRENT_TIMESTAMP AS load_timestamp
        
    FROM agent_calls
)

SELECT *
FROM transformed
