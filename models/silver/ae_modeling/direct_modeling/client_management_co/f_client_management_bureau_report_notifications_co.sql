{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.client_management_bureau_report_notifications_co
SELECT
    -- MANDATORY FIELDS
    loan_id,
    client_id,
    created_at,
    notification_updated_at,
    notification_id,
    bureau_report_id,
    notification_body,
    notification_type,
    notification_status,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('client_management_bureau_report_notifications_co') }}
