
--raw_modeling.prospectbureaupersonalinfoobtained_co
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
    COALESCE(json_tmp.prospectId, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.personId.ageRange AS personId_ageRange,
    json_tmp.personId.expeditionCity AS personId_expeditionCity,
    TO_TIMESTAMP(json_tmp.personId.expeditionDate) AS personId_expeditionDate,
    json_tmp.personId.firstName AS personId_firstName,
    json_tmp.personId.fullName AS personId_fullName,
    json_tmp.personId.lastName AS personId_lastName,
    json_tmp.personId.middleName AS personId_middleName,
    json_tmp.personId.number AS personId_number,
    json_tmp.personId.secondLastName AS personId_secondLastName,
    json_tmp.personId.status AS personId_status,
    json_tmp.personId.type AS personId_type,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- MANUAL MODELED DEPENDENCY
    json_tmp.personId.expeditionCity AS document_expedition_city,
    TO_TIMESTAMP(json_tmp.personId.expeditionDate) AS document_expedition_date,
    json_tmp.personId.fullName AS full_name,
    json_tmp.personId.number AS id_number,
    json_tmp.personId.type AS id_type,
    json_tmp.personId.lastName AS last_name,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureaupersonalinfoobtained_co_at -- To store it as a standalone column, when needed
FROM  raw_modeling.prospectbureaupersonalinfoobtained_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
