


--raw_modeling.privacypolicyaccepted_br
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
    COALESCE(
        json_tmp.application.allyId.slug,
        json_tmp.metadata.context.allyId
    ) AS ally_slug,
    COALESCE(
        json_tmp.application.applicationId.id,
        json_tmp.application.applicationId.value,
        json_tmp.metadata.context.traceId
    ) AS application_id,
    json_tmp.application.prospect.cellPhoneNumber AS application_cellphone,
    json_tmp.application.prospect.email.value AS application_email,
    json_tmp.application.prospect.birth.date AS birth_date,
    json_tmp.application.type AS application_channel_legacy,
    COALESCE(
        json_tmp.application.prospect.id.id,
        json_tmp.metadata.context.clientId
    ) AS client_id,
    json_tmp.application.prospect.lastName AS first_last_name,
    json_tmp.application.prospect.idNumber AS id_number,
    json_tmp.application.prospect.idType AS id_type,
    json_tmp.application.requestedAmount.value AS requested_amount,
    COALESCE(
              json_tmp.application.storeId.slug,
              json_tmp.metadata.context.storeId
    ) AS store_slug,
    COALESCE(
              json_tmp.metadata.context.userId,
              json_tmp.metadata.context.storeUserId
    ) AS store_user_id,

    -- CUSTOM ATTRIBUTES
    CAST(TRUE AS BOOLEAN) AS custom_is_privacy_policy_accepted,
    'LEGACY' as custom_platform_version
-- DBT SOURCE REFERENCE
FROM raw_modeling.privacypolicyaccepted_br
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
