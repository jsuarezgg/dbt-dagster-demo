
--raw_modeling.loancancellationorderprocessed_co
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
    json_tmp.cancellationId.id AS loan_cancellation_id,
    json_tmp.cancellationReason.value AS loan_cancellation_reason,
    TO_TIMESTAMP(json_tmp.cancellationOrderDate.value) AS loan_cancellation_order_date,
    COALESCE(json_tmp.allyId.slug,json_tmp.metadata.context.allyId) AS ally_slug,
    COALESCE(json_tmp.storeId.slug,json_tmp.metadata.context.storeId) AS store_slug,
    COALESCE(json_tmp.clientId.id, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.loanId.id AS loan_id,
    json_tmp.approvedAmount.value AS loan_approved_amount,
    TO_TIMESTAMP(json_tmp.originationDate.value) AS loan_origination_date,
    COALESCE(json_tmp.userId.id, json_tmp.metadata.context.userId) AS user_id,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'CLIENT_MANAGEMENT' AS custom_event_domain,
    'V1_CANCELLATION_PROCESSED' AS custom_loan_cancellation_status
    -- CAST(ocurred_on AS TIMESTAMP) AS loancancellationorderprocessed_co_at -- To store it as a standalone column, when needed
FROM  raw_modeling.loancancellationorderprocessed_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
