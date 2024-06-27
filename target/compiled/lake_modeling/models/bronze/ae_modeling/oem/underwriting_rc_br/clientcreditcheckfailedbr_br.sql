


--raw_modeling.clientcreditcheckfailedbr_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
      -- Fill with your mapped fields
    json_tmp.metadata.context.allyId as ally_slug,
    json_tmp.application.id as application_id,
    json_tmp.client.id as client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.application.product AS product,
    coalesce(cast(json_tmp.creditCheck.lowBalanceLoan as boolean),
             cast(json_tmp.creditCheck.lowBalanceLoanV2 as boolean)) as lbl,
    json_tmp.creditCheck.learningPopulation as learning_population,
    -- json_tmp.totalAddiCupo as client_max_exposure,
    json_tmp.creditCheck.income.netValue as credit_check_income_net_value,
    json_tmp.creditCheck.income.provider as credit_check_income_provider,
    json_tmp.creditCheck.income.type as credit_check_income_type,
    /* coalesce(json_tmp.income.validator.calculationMethod.value,
             json_tmp.income.validator.calculationMethod) as credit_check_income_validator_calculation_method,
    json_tmp.income.validator.contributionType.value as credit_check_income_validator_contribution_type, */ -- validator is a string
    json_tmp.creditCheck.creditPolicy.name as credit_policy_name,
    COALESCE(json_tmp.creditCheck.scorev2.name, json_tmp.creditCheck.score.name) as credit_score_name,
    json_tmp.creditCheck.scoreV2.name as credit_score_product,
    json_tmp.creditCheck.status as credit_status,
    json_tmp.creditCheck.statusReason as credit_status_reason,
    json_tmp.creditCheck.fraudModel.score as fraud_model_score,
    json_tmp.creditCheck.groupName as group_name,
    coalesce(json_tmp.creditCheck.score.pdCalculationMethod, 
             json_tmp.creditCheck.scorev2.pdCalculationMethod) as pd_calculation_method,
    json_tmp.creditCheck.scorev2.addiProbabilityDefault as probability_default_addi,
    json_tmp.creditCheck.scorev2.probabilityDefault as probability_default_bureau,
    /* coalesce(json_tmp.storeFraudRiskLevel.value,
             json_tmp.storeFraudRiskLevel) as store_fraud_risk_level, */ -- storeFraudRisk does not exist
    json_tmp.creditCheck.totalDebtToServiceRatio as tdsr,
    json_tmp.creditCheck.bypassRejectedReason as bypassed_reason,
    COALESCE(json_tmp.creditCheck.scorev2.value, json_tmp.creditCheck.score.value) as credit_score,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    cast(true as BOOLEAN) as returning_client
-- DBT SOURCE REFERENCE
from raw_modeling.clientcreditcheckfailedbr_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
