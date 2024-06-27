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

--raw_modeling.pixpaymentreceivedbr_br
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
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS origination_date, 
    CAST(json_tmp.application.requestedAmount AS DOUBLE) AS requested_amount,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.originationEventType AS event_type,
    COALESCE(json_tmp.ally.slug, json_tmp.metadata.context.allyId) AS ally_slug,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.product AS product,
    json_tmp.client.type AS client_type,
    json_tmp.application.channel AS channel,
    -- CUSTOM ATTRIBUTES
    'APPROVED' AS is_approval_custom_value
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'pixpaymentreceivedbr_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}