{{
    config(
        materialized='incremental',
        unique_key='message_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 149772366 by 2023-09-19
--bronze.kustomer_messages
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    message_id,
    channel,
    app,
    size,
    direction,
    direction_type,
    preview,
    meta,
    status,
    response_time,
    response_business_time,
    assigned_teams,
    assigned_users,
    auto,
    sent_at,
    created_at,
    updated_at,
    modified_at,
    redacted,
    created_by_teams,
    rev,
    reactions,
    intent_detections,
    org_id,
    created_by_id,
    modified_by_id,
    customer_id,
    conversation_id,
    attachment_id,
    reference_date,
    reference_date_year,
    reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_messages') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updated_at) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updated_at AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}