{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.operations_dialer_talkdesk_agent_times_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    date,
    agent_name,
    email,
    metric_name,
    total_calls,
    outbound_calls,
    inbound_calls,
    average_speed_to_answered,
    dial_attempts,
    pickup_rate,
    total_talk_time,
    average_talk_time,
    active_break,
    after_call_work,
    all_hands,
    another_channel,
    available,
    away,
    backup_time,
    break,
    daily_meeting,
    feedback,
    free_time,
    health_time,
    manual_call,
    offline,
    on_a_call,
    operative,
    quality_team,
    team_dialer,
    team_digital,
    team_it_support,
    training,
    whatsapp_channel,
    outbound_connected_calls,
    agent_disconnected,
    agent_Disconnected_perc,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'operations_dialer_talkdesk_agent_times_co') }}
