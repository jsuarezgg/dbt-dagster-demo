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
--raw_modeling.kus_messages
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    messageId AS message_id,
    channel AS channel,
    app AS app,
    size AS size,
    direction AS direction,
    directionType AS direction_type,
    NULLIF(TRIM(preview),'') AS preview,
    meta AS meta,
    status AS status,
    responseTime AS response_time,
    responseBusinessTime AS response_business_time,
    assignedTeams AS assigned_teams,
    assignedUsers AS assigned_users,
    auto AS auto,
    sentAt AS sent_at,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    redacted AS redacted,
    createdByTeams AS created_by_teams,
    rev AS rev,
    reactions AS reactions,
    intentDetections AS intent_detections,
    orgId AS org_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    customerId AS customer_id,
    conversationId AS conversation_id,
    attachmentId AS attachment_id,
    referenceDate AS reference_date,
    year AS reference_date_year,
    month AS reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_messages') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updatedAt) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updatedAt AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
