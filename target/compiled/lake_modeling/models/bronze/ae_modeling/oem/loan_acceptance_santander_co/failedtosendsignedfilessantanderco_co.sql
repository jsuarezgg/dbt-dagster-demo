


--raw_modeling.failedtosendsignedfilessantanderco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2024-06-26 22:26:15 +0000') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.ally.slug AS ally_slug,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    COALESCE(json_tmp.application.channel,json_tmp.metadata.context.channel) AS channel,
    json_tmp.application.product AS product,
    json_tmp.loan.approvedAmount as approved_amount,
    json_tmp.loan.guarantee.totalGuaranteeRate as guarantee_rate,
    json_tmp.loan.effectiveAnnualRate as effective_annual_rate,
    coalesce(cast(json_tmp.creditCheck.lowBalanceLoanV2 as boolean),
             cast(json_tmp.creditCheck.lowBalanceLoan as boolean)) as lbl,
    json_tmp.loan.id as loan_id,
    TO_TIMESTAMP(json_tmp.loan.originationDate) as origination_date,
    json_tmp.ally.store.slug as store_slug,
    json_tmp.user.id as store_user_id,
    json_tmp.loan.term as term,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(true as boolean) as custom_is_santander_originated,
    CAST(1 AS TINYINT) AS loan_acceptance_by_gateway_passed
    -- CAST(ocurred_on AS TIMESTAMP) AS failedtosendsignedfilessantanderco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from `addi_prod`.`raw_modeling`.`failedtosendsignedfilessantanderco_co`
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2024-06-26 22:26:00"- INTERVAL "40" HOUR)) AND to_date("2024-06-26 22:26:00")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2024-06-26 22:26:00"- INTERVAL "40" HOUR)) AND to_timestamp("2024-06-26 22:26:00")
