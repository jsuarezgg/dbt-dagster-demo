


--raw_modeling.clientloansstatusupdatedv2_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    to_date(ocurred_on) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    json_tmp.client.addiCupo.addiCupoLastUpdateReason as addicupo_last_update_reason,
    json_tmp.client.addiCupo.remainingBalance.value as addicupo_remaining_balance,
    json_tmp.client.addiCupo.source as addicupo_source,
    json_tmp.client.addiCupo.state as addicupo_state,
    json_tmp.client.addiCupo.totalAddiCupo.value as addicupo_total,
    json_tmp.client.clientId.value as client_id,
    json_tmp.client.status.clientDelinquencyBalance.value as delinquency_balance,
    json_tmp.mode as event_mode,
    json_tmp.version as event_version,
    json_tmp.client.status.clientFullPayment.value as full_payment,
    json_tmp.client.status.clientInstallmentPayment.value as installment_payment,
    json_tmp.client.status.clientMinPayment.value as min_payment,
    json_tmp.client.ownership as ownership,
    json_tmp.client.status.clientPayDay.value as payday,
    json_tmp.client.status.positiveBalance.value as positive_balance
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
from raw_modeling.clientloansstatusupdatedv2_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
