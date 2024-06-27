

--raw_modeling.clientpaymentannulledv2_co
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
    json_tmp.annulmentReason as annulment_reason,
    coalesce(json_tmp.clientId.id, json_tmp.metadata.context.clientId) as client_id,
    json_tmp.clientPaymentId.id as payment_id,
    -- CUSTOM ATTRIBUTES
    TRUE as is_annulled
-- DBT SOURCE REFERENCE
FROM raw_modeling.clientpaymentannulledv2_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
