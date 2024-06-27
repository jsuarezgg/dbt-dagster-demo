


--raw_modeling.prospectloanapplicationcreatedv2_co
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
    COALESCE(json_tmp.allyId.slug, json_tmp.metadata.context.allyId) AS ally_slug,
    json_tmp.applicationType AS application_channel_legacy,
    CAST(ocurred_on AS TIMESTAMP) AS application_date,
    COALESCE(json_tmp.applicationId.id, json_tmp.applicationId.value, json_tmp.metadata.context.traceId) AS application_id,
    COALESCE(json_tmp.prospectId.id, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.onlineApplicationOrder.orderId AS order_id,
    json_tmp.requestedAmount.value AS requested_amount,
    json_tmp.requestedAmountWithoutDiscount AS requested_amount_without_discount,
    COALESCE(json_tmp.storeId.slug, json_tmp.metadata.context.storeId) AS store_slug,
    COALESCE(json_tmp.metadata.context.userId, json_tmp.metadata.context.storeUserId) AS store_user_id,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    -- CUSTOM ATTRIBUTES
    'LEGACY' AS custom_platform_version
-- DBT SOURCE REFERENCE
FROM raw_modeling.prospectloanapplicationcreatedv2_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
