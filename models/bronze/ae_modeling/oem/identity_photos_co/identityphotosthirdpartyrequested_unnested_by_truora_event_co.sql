
{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.identityphotosthirdpartyrequested_co
WITH select_explode AS (
    SELECT
        -- MANDATORY FIELDS
        json_tmp.eventType AS event_name_original,
        reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
        json_tmp.eventId AS event_id,
        CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
        to_date(json_tmp.ocurredOn) AS ocurred_on_date,
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at,
        -- MAPPED FIELDS - DIRECT ATTRIBUTES
        json_tmp.application.id AS application_id,
        json_tmp.client.id AS client_id,
        EXPLODE(json_tmp.identityVerification.thirdParty.truora.events) AS truora_event,
        -- CUSTOM ATTRIBUTES
        'V4' as custom_idv_version
    -- DBT SOURCE REFERENCE
    from {{ source('raw_modeling', 'identityphotosthirdpartyrequested_co') }}
    -- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}

)

SELECT 
    CONCAT('EID_',event_id,'_TEVID_',truora_event.validationId) AS surrogate_key,
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	application_id,
	client_id,
	truora_event.validationId AS truora_event_validation_id,
	truora_event.validationStatus AS truora_event_validation_status,
	truora_event.confidenceScore AS truora_event_confidence_score,
	truora_event.declinedReason AS truora_event_declined_reason,
	truora_event.failureStatus AS truora_event_failure_status,
	-- truora_event.id AS trora_event_id,
	truora_event.threshold AS truora_event_threshold,
	to_timestamp(truora_event.timestamp) AS truora_event_timestamp,
	truora_event.type AS truora_event_type
FROM select_explode