{{
    config(
        materialized='incremental',
        unique_key='work_item_id',
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
    workItemId AS work_item_id,
    resourceType AS resource_type,
    resourceId AS resource_id,
    status AS status,
    paused AS paused,
    channel AS channel,
    firstEnterQueueAt AS first_enter_queue_at,
    queuedCount AS queued_count,
    priority AS priority,
    itemSize AS item_size,
    ivrBusinessTime AS ivr_business_time,
    handleBusinessTime AS handle_business_time,
    handleCompletedAt AS handle_completed_at,
    wrapUpBusinessTime AS wrap_up_business_time,
    wrapUpEnteredAt AS wrap_up_entered_at,
    completedAt AS completed_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    createdAt AS created_at,
    resourceRev AS resource_rev,
    resourceCreatedAt AS resource_created_at,
    resourceDirection AS resource_direction,
    resourceFirstQueueTime AS resource_first_queue_time,
    resourceFirstRouteTime AS resource_first_route_time,
    resourceFirstAssignTime AS resource_first_assign_time,
    rev AS rev,
    firstRoutedResponseBusinessTime AS first_routed_response_business_time,
    firstRoutedResponseTime AS first_routed_response_time,
    firstRoutedResponseId AS first_routed_response_id,
    firstRoutedResponseCreatedAt AS first_routed_response_created_at,
    lastRevisionEnteredQueueAt AS last_revision_entered_queue_at,
    lastRevisionQueueTime AS last_revision_queue_time,
    lastRevisionQueueBusinessTime AS last_revision_queue_business_time,
    lastRevisionRoutedAt AS last_revision_routed_at,
    lastRevisionAcceptedAt AS last_revision_accepted_at,
    orgId AS org_id,
    modifiedById AS modified_by_id,
    teamId AS team_id,
    workSessionId AS work_session_id,
    routedToSessionId AS routed_to_session_id,
    routedToId AS routed_to_id,
    lastRevisionId AS last_revision_id,
    assignedToId AS assigned_to_id,
    queueId AS queue_id,
    acceptedById AS accepted_by_id,
    referenceDate AS reference_date,
    year AS reference_date_year,
    month AS reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_work_items') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updatedAt) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updatedAt AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
