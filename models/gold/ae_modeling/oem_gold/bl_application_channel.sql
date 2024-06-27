{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- AE - Carlos D. Puerto: Incremental approach as the one we use with the silver builder
WITH
{%- if is_incremental() %}
target_applications_co AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_co') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
target_applications_br AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_br') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_applications_co AS (
    SELECT *
    FROM {{ ref('f_applications_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_br AS (
    SELECT *
    FROM {{ ref('f_applications_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
),
f_applications AS (
    SELECT 'BR' as country_code, application_id, channel, application_channel_legacy, application_date, custom_platform_version FROM f_applications_br
    UNION ALL
    SELECT 'CO' as country_code, application_id, channel, application_channel_legacy, application_date, custom_platform_version FROM f_applications_co
)
SELECT
    country_code,
    application_id,
    COALESCE(channel,application_channel_legacy) AS application_channel,
    CASE
        WHEN COALESCE(channel,application_channel_legacy) ILIKE '%PAY%LINK%' THEN 'PAYLINK'
        WHEN COALESCE(channel,application_channel_legacy) ILIKE '%E%COMMERCE%' THEN 'E_COMMERCE'
        WHEN COALESCE(channel,application_channel_legacy) ILIKE '%PRE%APPROVAL%' THEN 'PREAPPROVAL'
        WHEN COALESCE(channel,application_channel_legacy) ILIKE '%IN%STORE%' THEN 'PAYLINK'
        WHEN COALESCE(channel,application_channel_legacy) IN ('ONLINE_LOAN','POS_LOAN') THEN 'LEGACY_PENDING_MAPPING'
        WHEN COALESCE(channel,application_channel_legacy) ILIKE '%REFINANCE%' THEN 'REFINANCE'
        WHEN COALESCE(channel,application_channel_legacy) ILIKE '%ADDI_MARKETPLACE%' THEN 'E_COMMERCE'
        WHEN COALESCE(channel,application_channel_legacy) IS null THEN 'LEGACY_UNKNOWN'
    ELSE '_PENDING_MAPPING_'
    END AS synthetic_channel,
    NAMED_STRUCT('custom_platform_version',custom_platform_version,
              'application_date',application_date,
              'application_channel_legacy',application_channel_legacy) AS debug_data,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM f_applications
