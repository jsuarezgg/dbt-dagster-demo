


--raw_modeling.privacypolicyaccepted_co
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
    COALESCE(json_tmp.loanApplication.prospect.expeditionDate,json_tmp.application.prospect.expeditionDate) AS document_expedition_date,
    COALESCE(json_tmp.loanApplication.prospect.idNumber,json_tmp.application.prospect.idNumber) AS id_number,
    COALESCE(json_tmp.loanApplication.prospect.idType,json_tmp.application.prospect.idType) AS id_type,
    COALESCE(json_tmp.loanApplication.prospect.lastName,json_tmp.application.prospect.lastName) AS last_name,
    COALESCE(json_tmp.loanApplication.prospect.id.id,json_tmp.application.prospect.id.id,json_tmp.metadata.context.clientId) AS client_id,
    COALESCE(json_tmp.loanApplication.applicationId.id, json_tmp.application.applicationId.id, json_tmp.metadata.context.traceId) AS application_id,
    CAST(ocurred_on AS TIMESTAMP) AS application_date,
    COALESCE(json_tmp.loanApplication.prospect.cellPhoneNumber, json_tmp.application.prospect.cellPhoneNumber) AS application_cellphone,
    json_tmp.application.prospect.email.value AS application_email,
    COALESCE(json_tmp.loanApplication.storeId.slug, json_tmp.application.storeId.slug, json_tmp.metadata.context.storeId) AS store_slug,
    COALESCE(json_tmp.metadata.context.userId, json_tmp.metadata.context.storeUserId) AS store_user_id,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    COALESCE(json_tmp.loanApplication.requestedAmount, json_tmp.application.requestedAmount.value) AS requested_amount,
    -- CUSTOM ATTRIBUTES
    'V1' as custom_idv_version,
    CAST(TRUE AS BOOLEAN) AS custom_is_privacy_policy_accepted,
    NAMED_STRUCT(
        'event_id', json_tmp.eventId,
        'ocurred_on', ocurred_on::TIMESTAMP,
        'privacy',
            named_struct(
            "isAccepted", CAST(TRUE AS BOOLEAN)
            )
    ) AS privacy_policy_detail_json
-- DBT SOURCE REFERENCE
FROM raw_modeling.privacypolicyaccepted_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
