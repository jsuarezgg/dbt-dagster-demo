


--raw_modeling.conditionalcreditcheckpassedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
      -- Fill with your mapped fields
    json_tmp.metadata.context.allyId as ally_slug,
    json_tmp.creditCheck.bypassRejectedReason AS bypassed_reason,
    json_tmp.creditCheck.shouldSkipIdv AS should_skip_idv,
    json_tmp.application.id as application_id,
    json_tmp.client.id as client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    COALESCE(json_tmp.application.channel,json_tmp.metadata.context.channel) AS channel,
    json_tmp.application.product AS product,
    coalesce(cast(json_tmp.creditCheck.lowBalanceLoan as boolean),
             cast(json_tmp.creditCheck.lowBalanceLoanV2 as boolean)) as lbl,
    json_tmp.creditCheck.learningPopulation as learning_population,
    json_tmp.creditCheck.totalADDICupo as client_max_exposure,
    json_tmp.creditCheck.income.netValue as credit_check_income_net_value,
    json_tmp.creditCheck.income.provider as credit_check_income_provider,
    json_tmp.creditCheck.income.type as credit_check_income_type,
    -- json_tmp.creditCheck.income.validator.calculationMethod as credit_check_income_validator_calculation_method,
    -- json_tmp.creditCheck.income.validator.contributionType as credit_check_income_validator_contribution_type,
    json_tmp.creditCheck.creditPolicy.name as credit_policy_name,
    coalesce(json_tmp.creditCheck.scoreV2.value,
             json_tmp.creditCheck.score.value) as credit_score,
    coalesce(json_tmp.creditCheck.scoreV2.name,
             json_tmp.creditCheck.score.name) as credit_score_name,
    -- json_tmp.creditScoreV2.name.value as credit_score_product,
    json_tmp.creditCheck.status as credit_status,
    json_tmp.creditCheck.statusReason as credit_status_reason,
    json_tmp.creditCheck.fraudModel.score as fraud_model_score,
    json_tmp.creditCheck.fraudModel.version as fraud_model_version,
    json_tmp.creditCheck.groupName as group_name,
    json_tmp.psychometricAssessment.evaluation.overallScore as ia_overall_score,
    coalesce(json_tmp.creditCheck.scoreV2.pdCalculationMethod,
             json_tmp.creditCheck.score.pdCalculationMethod) as pd_calculation_method,
    json_tmp.creditCheck.fraudModel.policy as policy,
    coalesce(json_tmp.creditCheck.scoreV2.addiProbabilityDefault,
             json_tmp.creditCheck.score.addiProbabilityDefault) as probability_default_addi,
    coalesce(json_tmp.creditCheck.scoreV2.probabilityDefault,
             json_tmp.creditCheck.score.probabilityDefault) as probability_default_bureau,
    cast(json_tmp.creditCheck.shouldBeBlackListed as boolean) as should_be_black_listed,
    json_tmp.creditCheck.fraudModel.riskLevel as store_fraud_risk_level,
    json_tmp.creditCheck.totalDebtToServiceRatio as tdsr,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(1 AS TINYINT) AS conditional_credit_check_passed,
    CAST(false AS BOOLEAN) as returning_client
-- DBT SOURCE REFERENCE
from raw_modeling.conditionalcreditcheckpassedco_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
