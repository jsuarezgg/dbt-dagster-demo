
--raw_modeling.loancancellationorderprocessedv2_br
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
    json_tmp.cancellationAmount AS loan_cancellation_amount,
    json_tmp.cancellationType AS loan_cancellation_type,
    json_tmp.cancellationReason.value AS loan_cancellation_reason,
    TO_TIMESTAMP(json_tmp.cancellationOrderDate.value) AS loan_cancellation_order_date,
    COALESCE(json_tmp.allyId.slug,json_tmp.metadata.context.allyId) AS ally_slug,
    COALESCE(json_tmp.storeId.slug,json_tmp.metadata.context.storeId)  AS store_slug,
    COALESCE(json_tmp.clientId.id,json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.loanId.id AS loan_id,
    json_tmp.approvedAmount.value AS loan_approved_amount,
    TO_TIMESTAMP(json_tmp.originationDate.value) AS loan_origination_date,
    COALESCE(json_tmp.userId.id,json_tmp.metadata.context.userId) AS user_id,
    json_tmp.metadata.context.userRole AS user_role,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    -- CUSTOM ATTRIBUTES
    'CLIENT_MANAGEMENT' AS custom_event_domain,
    'V2_CANCELLATION_PROCESSED' AS custom_loan_cancellation_status,
    -- CAST(ocurred_on AS TIMESTAMP) AS loancancellationorderprocessedv2_co_at -- To store it as a standalone column, when needed

    --- OLD MAPPED FIELDS, TO DEPRECATE IN NEAR FUTURE
    json_tmp.cancellationOrderDate.value AS cancellation_date,
    json_tmp.cancellationReason.value AS cancellation_reason,
    json_tmp.cancellationId.id as cancellation_id
FROM  raw_modeling.loancancellationorderprocessedv2_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
