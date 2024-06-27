{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.idvthirdpartyapproved_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.originationEventType AS event_type,
    json_tmp.metadata.context.allyId AS ally_slug, -- alternative: json_tmp.metadata.context.allyId
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.product AS product,
    json_tmp.client.type AS client_type,
    json_tmp.application.channel AS channel,
    CAST(json_tmp.idvThirdParty.createdAt AS TIMESTAMP) AS idv_tp_created_at,
    json_tmp.idvThirdParty.integration.attempt AS idv_tp_integration_attempt,
    json_tmp.idvThirdParty.integration.errorCount AS idv_tp_integration_error_count,
    CAST(json_tmp.idvThirdParty.integration.lastAttemptAt AS TIMESTAMP) AS idv_tp_integration_last_attempt_at,
    json_tmp.idvThirdParty.integration.unico.errorCode AS idv_tp_unico_error_code,
    json_tmp.idvThirdParty.integration.unico.processId AS idv_tp_unico_process_id,
    json_tmp.idvThirdParty.integration.unico.score AS idv_tp_unico_score,
    json_tmp.idvThirdParty.integration.unico.status AS idv_tp_unico_status,
    json_tmp.idvThirdParty.integration.unico.tokenLocation AS idv_tp_token_location,
    json_tmp.idvThirdParty.provider AS idv_tp_provider,
    CAST(json_tmp.idvThirdParty.updatedAt AS TIMESTAMP) AS idv_tp_updated_at,
    json_tmp.metadata.context.ipAddress.value as ip_address,
    json_tmp.metadata.context.userAgent.value as user_agent,
    -- CUSTOM ATTRIBUTES
    'APPROVED' AS idv_tp_custom_last_status,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS idvthirdpartyapproved_br_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'idvthirdpartyapproved_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}