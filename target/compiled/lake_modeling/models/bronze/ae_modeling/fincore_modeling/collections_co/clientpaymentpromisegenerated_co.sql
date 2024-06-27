

--raw_modeling.clientpaymentpromisegenerated_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.paymentPromise.id.value AS payment_promise_id, 
    COALESCE(json_tmp.client.id.value, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.paymentPromise.state AS state,
    json_tmp.paymentPromise.expectedAmount.value AS expected_amount,
    CAST(json_tmp.paymentPromise.startDate.value AS TIMESTAMP) AS start_date,
    CAST(json_tmp.paymentPromise.endDate.value AS TIMESTAMP) AS end_date,
    json_tmp.resolutionCall AS resolution_call,
    json_tmp.agent.id.value AS agent_info,
    json_tmp.conditions AS conditions,
    -- CUSTOM ATTRIBUTES
    'ClientPaymentPromiseGenerated' AS stage
-- DBT SOURCE REFERENCE
FROM raw_modeling.clientpaymentpromisegenerated_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
