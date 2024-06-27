
--raw_modeling.prospectbureaucontactinfoobtained_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.applicationId AS application_id,
    json_tmp.prospectId AS client_id,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureaucontactinfoobtained_br_at -- To store it as a standalone column, when needed
FROM  raw_modeling.prospectbureaucontactinfoobtained_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
