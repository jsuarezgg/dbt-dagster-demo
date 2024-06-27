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
--bronze.kustomer_work_items
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    work_item_id,
    resource_type,
    resource_id,
    status,
    paused,
    channel,
    first_enter_queue_at,
    queued_count,
    priority,
    item_size,
    ivr_business_time,
    handle_business_time,
    handle_completed_at,
    wrap_up_business_time,
    wrap_up_entered_at,
    completed_at,
    updated_at,
    modified_at,
    created_at,
    resource_rev,
    resource_created_at,
    resource_direction,
    resource_first_queue_time,
    resource_first_route_time,
    resource_first_assign_time,
    rev,
    first_routed_response_business_time,
    first_routed_response_time,
    first_routed_response_id,
    first_routed_response_created_at,
    last_revision_entered_queue_at,
    last_revision_queue_time,
    last_revision_queue_business_time,
    last_revision_routed_at,
    last_revision_accepted_at,
    org_id,
    modified_by_id,
    team_id,
    work_session_id,
    routed_to_session_id,
    routed_to_id,
    last_revision_id,
    assigned_to_id,
    queue_id,
    accepted_by_id,
    reference_date,
    reference_date_year,
    reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_work_items') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updated_at) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updated_at AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}