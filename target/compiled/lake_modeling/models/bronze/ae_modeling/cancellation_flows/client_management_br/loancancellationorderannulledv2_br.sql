
--raw_modeling.loancancellationorderannulledv2_br
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
    json_tmp.annulmentReason AS loan_cancellation_annulment_reason,
    json_tmp.cancellationId.id AS loan_cancellation_id,
    json_tmp.clientId.id AS client_id,
    json_tmp.loanId.id AS loan_id,
    json_tmp.metadata.context.userId AS user_id,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    -- CUSTOM ATTRIBUTES
    'CLIENT_MANAGEMENT' AS custom_event_domain,
    'V2_CANCELLATION_ANNULLED' AS custom_loan_cancellation_status,
    -- CAST(ocurred_on AS TIMESTAMP) AS loancancellationorderannulledv2_br_at -- To store it as a standalone column, when needed

    --- OLD MAPPED FIELDS, TO DEPRECATE IN NEAR FUTURE
    json_tmp.annulmentReason AS annulment_reason,
    json_tmp.cancellationId.id as cancellation_id
FROM  raw_modeling.loancancellationorderannulledv2_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
