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
    work_session_id,
    routable,
    status_type,
    work_item_count,
    paused_work_item_count,
    signed_in_at,
    signed_out_at,
    capacity,
    capacity_remaining,
    total_capacity,
    handled_item_count,
    handled_conversation_count,
    total_available_status_at,
    total_available_business_time,
    total_available_idle_capacity_business_time,
    total_available_not_at_capacity_business_time,
    total_available_at_capacity_business_time,
    total_unavailable_status_at,
    total_unavailable_business_time,
    total_unavailable_idle_capacity_business_time,
    total_unavailable_not_at_capacity_business_time,
    total_unavailable_at_capacity_business_time,
    total_time_by_status,
    capacity_status,
    last_assigned_item_at,
    updated_at,
    modified_at,
    created_at,
    rev,
    last_revision_handled_item_count,
    org_id,
    user_id,
    team_id,
    routing_settings_id,
    status_id,
    modified_by_id,
    queue_id,
    last_revision_id,
    reference_date,
    reference_date_year,
    reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_work_sessions') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updated_at) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updated_at AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}