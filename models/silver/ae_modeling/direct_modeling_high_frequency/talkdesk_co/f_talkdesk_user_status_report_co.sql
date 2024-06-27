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
    user_name,
    user_email,
    user_id,
    status_label,
    status_start_at,
    status_end_at,
    status_time,
    is_user_active,
    team_id,
    team_name,
    audit_time_insert_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('talkdesk_user_status_report_co') }}
