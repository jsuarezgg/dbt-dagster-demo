{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_payments_payment_reminder_notifications_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id AS payment_reminder_notification_id,
    client_id,
    notification_id,
    notification_date,
    notification_type,
    notification_updated_at,
    notification_body,
    days_to_pay,
    cast(is_automatic_debit as BOOLEAN) as is_automatic_debit,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_payments_payment_reminder_notifications_co') }}
