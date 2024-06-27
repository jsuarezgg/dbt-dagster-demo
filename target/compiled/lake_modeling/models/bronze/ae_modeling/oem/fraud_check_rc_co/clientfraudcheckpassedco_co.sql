


--raw_modeling.clientfraudcheckpassedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.ally.slug AS ally_slug,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.application.product AS product
    -- json_tmp.creditCheck.totalADDICupo as client_max_exposure,
    -- json_tmp.creditCheck.income.netValue as credit_check_income_net_value,
    -- json_tmp.creditCheck.income.provider as credit_check_income_provider,
    -- json_tmp.creditCheck.income.type as credit_check_income_type,
    -- json_tmp.creditCheck.income.validator.calculationMethod as credit_check_income_validator_calculation_method,
    -- json_tmp.creditCheck.income.validator.contributionType as credit_check_income_validator_contribution_type,
    -- json_tmp.creditCheck.creditPolicy.name as credit_policy_name.
    /* coalesce(json_tmp.creditCheck.scoreV2.value,
             json_tmp.creditCheck.score.value) as credit_score,
    coalesce(json_tmp.creditCheck.scoreV2.name,
             json_tmp.creditCheck.score.name) as credit_score_name, */
    -- json_tmp.creditScoreV2.name.value as credit_score_product,
    -- json_tmp.fraudCheck.status as credit_status,
    -- json_tmp.fraudCheck.statusReason as credit_status_reason
    -- json_tmp.creditCheck.fraudModel.score as fraud_model_score,
    -- json_tmp.creditCheck.groupName as group_name,
    -- json_tmp.psychometricAssessment.evaluation.overallScore as ia_overall_score,
    -- json_tmp.creditCheck.learningPopulation as learning_population,
    /*coalesce(json_tmp.creditCheck.scoreV2.pdCalculationMethod,
             json_tmp.creditCheck.score.pdCalculationMethod) as pd_calculation_method,
    coalesce(json_tmp.creditCheck.scoreV2.addiProbabilityDefault,
             json_tmp.creditCheck.score.addiProbabilityDefault) as probability_default_addi,
    coalesce(json_tmp.creditCheck.scoreV2.probabilityDefault,
             json_tmp.creditCheck.score.probabilityDefault) as probability_default_bureau, */
    -- json_tmp.creditCheck.fraudModel.riskLevel as store_fraud_risk_level,
    -- json_tmp.creditCheck.totalDebtToServiceRatio as tdsr
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS clientfraudcheckpassedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from raw_modeling.clientfraudcheckpassedco_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
