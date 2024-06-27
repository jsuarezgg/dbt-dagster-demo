{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    client_id,
    call_id,
    id_number,
    type,
    start_at,
    end_at,
    talkdesk_phone_number,
    talkdesk_phone_display_name,
    contact_phone_number,
    user_id,
    user_name,
    user_email,
    total_time,
    talk_time,
    wait_time,
    hold_time,
    abandon_time,
    total_ringing_time,
    disposition_code,
    notes,
    user_voice_rating,
    ring_groups,
    ivr_options,
    is_in_business_hours,
    is_callback_from_queue,
    is_transfer,
    handling_user_name,
    recording_url,
    csat_score,
    csat_survey_time,
    mood,
    is_mood_prompted,
    team_id,
    team_name,
    rating_reason,
    outbound_dialer_campaigns_name,
    outbound_dialer_system_disposition,
    audit_time_insert_date,
    start_at_local_time,
    end_at_local_time,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('talkdesk_calls_report_co') }}
