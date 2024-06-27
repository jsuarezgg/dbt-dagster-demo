
--raw_modeling.prospectbureaupersonalinfoobtained_br
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
    COALESCE(json_tmp.prospectId,json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.personId.birth.city AS personId_birth_city,
    TO_TIMESTAMP(json_tmp.personId.birth.date) AS personId_birth_date,
    json_tmp.personId.birth.state AS personId_birth_state,
    json_tmp.personId.education AS personId_education,
    json_tmp.personId.firstName AS personId_firstName,
    json_tmp.personId.fullName AS personId_fullName,
    json_tmp.personId.gender AS personId_gender,
    TO_TIMESTAMP(json_tmp.personId.idUpdateDate) AS personId_idUpdateDate,
    json_tmp.personId.lastName AS personId_lastName,
    json_tmp.personId.maritalStatus AS personId_maritalStatus,
    json_tmp.personId.mother.name.full AS personId_mother_name_full,
    json_tmp.personId.number AS personId_number,
    json_tmp.personId.numberDependents AS personId_numberDependents,
    json_tmp.personId.partnerIdNumber AS personId_partnerIdNumber,
    json_tmp.personId.status AS personId_status,
    json_tmp.personId.type AS personId_type,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- MANUAL MODELED DEPENDENCY
    json_tmp.personId.birth.city AS birth_city,
    TO_TIMESTAMP(json_tmp.personId.birth.date) AS birth_date,
    json_tmp.personId.lastName AS first_name,
    json_tmp.personId.fullName AS full_name,
    json_tmp.personId.number AS id_number,
    json_tmp.personId.type AS id_type,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureaupersonalinfoobtained_br_at -- To store it as a standalone column, when needed
FROM  raw_modeling.prospectbureaupersonalinfoobtained_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
