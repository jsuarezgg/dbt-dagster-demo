


--raw_modeling.identityphotosthirdpartyapproved_co
WITH select_explode AS (
    SELECT
        -- MANDATORY FIELDS
        json_tmp.eventType AS event_name_original,
        reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
        json_tmp.eventId AS event_id,
        CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
        to_date(json_tmp.ocurredOn) AS ocurred_on_date,
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at,
        -- MAPPED FIELDS - DIRECT ATTRIBUTES
        json_tmp.application.id AS application_id,
        json_tmp.client.id AS client_id,
        EXPLODE(json_tmp.identityVerification.thirdParty.truora.events) AS truora_event
        -- CUSTOM ATTRIBUTES
          -- Fill with your custom attributes
    -- DBT SOURCE REFERENCE
    from raw_modeling.identityphotosthirdpartyapproved_co
    -- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")


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