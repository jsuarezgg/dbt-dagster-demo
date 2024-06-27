

--raw_modeling.applicationexpired_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.metadata.context.allyId AS ally_slug, -- alternative: json_tmp.metadata.context.allyId
    json_tmp.originationEventType AS event_type,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.product AS product,
    json_tmp.client.type AS client_type,
    json_tmp.application.channel AS channel,
    -- CUSTOM ATTRIBUTES
    'EXPIRED' AS termination_event_custom_name
-- DBT SOURCE REFERENCE
FROM raw_modeling.applicationexpired_br
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
