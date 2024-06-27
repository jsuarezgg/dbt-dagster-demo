
--raw_modeling.pixpaymentrefunded_br
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
    json_tmp.metadata.context.clientId AS client_id,
    json_tmp.metadata.context.storeId AS store_slug,
    json_tmp.metadata.context.userId AS user_id,
    json_tmp.pixPayment.amount AS pix_payment_amount,
    json_tmp.pixPayment.number AS pix_payment_number,
    TO_TIMESTAMP(json_tmp.refund.occurredOn) AS pix_payment_refund_occurred_on,
    json_tmp.refund.reason AS pix_payment_refund_reason,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'CLIENT_PAYMENTS' AS custom_event_domain,
    'PIX_REFUNDED' AS custom_payment_refund_status
    -- CAST(ocurred_on AS TIMESTAMP) AS pixpaymentrefunded_br_at -- To store it as a standalone column, when needed
FROM  raw_modeling.pixpaymentrefunded_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
