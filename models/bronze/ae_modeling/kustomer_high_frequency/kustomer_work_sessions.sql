{{
    config(
        materialized='incremental',
        unique_key='work_session_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 133010 by 2023-09-19
--raw_modeling.kustomer_work_sessions
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    workSessionId AS work_session_id,
    routable AS routable,
    statusType AS status_type,
    workItemCount AS work_item_count,
    pausedWorkItemCount AS paused_work_item_count,
    signedInAt AS signed_in_at,
    signedOutAt AS signed_out_at,
    capacity AS capacity,
    capacityRemaining AS capacity_remaining,
    totalCapacity AS total_capacity,
    handledItemCount AS handled_item_count,
    handledConversationCount AS handled_conversation_count,
    totalAvailable_statusAt AS total_available_status_at,
    totalAvailable_businessTime AS total_available_business_time,
    totalAvailableIdleCapacity_businessTime AS total_available_idle_capacity_business_time,
    totalAvailableNotAtCapacity_businessTime AS total_available_not_at_capacity_business_time,
    totalAvailableAtCapacity_businessTime AS total_available_at_capacity_business_time,
    totalUnavailable_statusAt AS total_unavailable_status_at,
    totalUnavailable_businessTime AS total_unavailable_business_time,
    totalUnavailableIdleCapacity_businessTime AS total_unavailable_idle_capacity_business_time,
    totalUnavailableNotAtCapacity_businessTime AS total_unavailable_not_at_capacity_business_time,
    totalUnavailableAtCapacity_businessTime AS total_unavailable_at_capacity_business_time,
    totalTimeByStatus AS total_time_by_status,
    capacityStatus AS capacity_status,
    lastAssignedItemAt AS last_assigned_item_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    createdAt AS created_at,
    rev AS rev,
    lastRevision_handledItemCount AS last_revision_handled_item_count,
    orgId AS org_id,
    userId AS user_id,
    teamId AS team_id,
    routingSettingsId AS routing_settings_id,
    statusId AS status_id,
    modifiedById AS modified_by_id,
    queueId AS queue_id,
    lastRevisionId AS last_revision_id,
    referenceDate AS reference_date,
    year AS reference_date_year,
    month AS reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_work_sessions') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updatedAt) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updatedAt AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
