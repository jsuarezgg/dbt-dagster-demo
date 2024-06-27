


--raw_modeling.privacypolicysent_co
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
    json_tmp.metadata.context.allyId AS ally_slug,
    COALESCE(json_tmp.applicationId.id, json_tmp.applicationId.value, json_tmp.metadata.context.traceId) AS application_id,
    CAST(ocurred_on AS TIMESTAMP) AS application_date,
    json_tmp.cellPhoneNumber AS application_cellphone,
    COALESCE(json_tmp.prospectId.id, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.metadata.context.storeId AS store_slug,
    COALESCE(json_tmp.metadata.context.userId, json_tmp.metadata.context.storeUserId) AS store_user_id,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    -- CUSTOM ATTRIBUTES
    'LEGACY' AS custom_platform_version
-- DBT SOURCE REFERENCE
FROM raw_modeling.privacypolicysent_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
