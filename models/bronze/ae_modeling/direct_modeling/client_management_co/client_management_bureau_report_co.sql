{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_management_bureau_report_co
SELECT
    -- MANDATORY FIELDS
    id AS bureau_report_id,
    started_at,
    finished_at,
    process_completed,
    number_of_clients_to_notify,
    number_of_notifications_sent,
    number_of_notifications_delivered,
    number_of_notifications_not_delivered,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_management_bureau_report_co') }}
