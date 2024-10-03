{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT
    notification_id,
    communication_id,
    notification_status,
    notification_updated_at,
    id,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('communications_notifications_status_co') }}
{% if is_incremental() %}
WHERE notification_updated_at BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date') }}")
{% endif %}
