{{ config(
    materialized='table',
    schema='jelte_stg_calls'
) }}

WITH raw AS (
    SELECT
        *
    FROM {{ source('jelte_raw', 'd_agent_by_skillset_stat') }}
)

SELECT
    -- renaming the columns in proper format
    raw.Intern_ID                       AS intern_id,
    raw.AgentGivenName                  AS agent_given_name,
    raw.AgentLogin                      AS agent_login,
    raw.AgentSurName                    AS agent_surname,
    raw.BlendedActiveTime               AS blended_active_time,
    raw.Call_Only                       AS call_only,        
    raw.Call_Type                       AS call_type,
    raw.CallsAnswered                   AS calls_answered,
    raw.CallsConferenced                AS calls_conferenced,
    raw.CallsOffered                    AS calls_offered,
    raw.CallsReturnToQ                  AS calls_return_to_q,
    raw.CallsReturnToQDueToTimeout      AS calls_return_to_q_due_to_timeout,
    raw.CallsTransferred                AS calls_transferred,
    raw.ConsultTime                     AS consult_time,
    raw.ContactHoldTime                 AS contact_hold_time,
    raw.ContactTalkTime                 AS contact_talk_time,
    raw.ContactType                     AS contact_type,
    raw.DNOutExtTalkTime               AS dn_out_ext_talk_time,
    raw.DNOutIntTalkTime               AS dn_out_int_talk_time,
    raw.HoldTime                        AS hold_time,

    -- Columns from page 2
    raw.IdleTime                        AS idle_time,
    raw.Lastname                        AS last_name,
    raw.MaxCapacityIdleTime             AS max_capacity_idle_time,
    raw.MaxCapacityTotalStaffedTime     AS max_capacity_total_staffed_time,
    raw.NotReadyTime                    AS not_ready_time,
    raw.PostCallProcessingTime          AS post_call_processing_time,
    raw.RingTime                        AS ring_time,
    raw.ShortCallsAnswered              AS short_calls_answered,
    raw.Site                            AS site,
    raw.SiteID                          AS site_id,
    raw.Skillset                        AS skillset,
    raw.SkillsetID                      AS skillset_id,
    raw.SubIdleTime_Voice               AS sub_idle_time_voice,
    raw.SubIdleTime_WC                 AS sub_idle_time_wc,
    raw.TalkTime                        AS talk_time,
    raw.Time                            AS time_event,          
    raw.Timestamp                       AS event_timestamp,
    raw.TotalStaffedTime                AS total_staffed_time,
    raw.UserID                          AS user_id,
    raw.Werkdag                         AS werkdag,

    
    raw.WaitTime                        AS wait_time,
    raw.Werkweek                        AS werkweek

FROM raw
