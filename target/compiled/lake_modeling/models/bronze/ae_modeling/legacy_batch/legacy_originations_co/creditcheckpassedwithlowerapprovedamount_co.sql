


--raw_modeling.creditcheckpassedwithlowerapprovedamount_co
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
    coalesce(json_tmp.application.applicationId.id,
             json_tmp.application.applicationId.value,
             json_tmp.metadata.context.traceId) as application_id,
    json_tmp.creditCheck.totalAddiCupo.value as client_max_exposure,
    json_tmp.creditCheck.income.netValue.value as credit_check_income_net_value,
    json_tmp.creditCheck.income.provider.value as credit_check_income_provider,
    json_tmp.creditCheck.income.type.value as credit_check_income_type,
    json_tmp.creditCheck.income.validator.calculationMethod as credit_check_income_validator_calculation_method,
    json_tmp.creditCheck.income.validator.contributionType.value as credit_check_income_validator_contribution_type,
    json_tmp.creditCheck.creditPolicyName as credit_policy_name,
    coalesce(json_tmp.creditCheck.creditScore.value,
             json_tmp.creditCheck.creditScoreV2.value.value) as credit_score,
    json_tmp.creditCheck.creditScoreName.value as credit_score_name,
    json_tmp.creditCheck.creditScoreV2.name.value as credit_score_product,
    json_tmp.creditCheck.status.value as credit_status,
    json_tmp.creditCheck.statusReason.value as credit_status_reason,
    json_tmp.creditCheck.fraudModelScore.value as fraud_model_score,
    json_tmp.creditCheck.fraudModelVersion.value as fraud_model_version,
    json_tmp.creditCheck.groupName.value as group_name,
    json_tmp.creditCheck.learningPopulation.value as learning_population,
    json_tmp.creditCheck.creditScoreV2.pdCalculationMethod as pd_calculation_method,
    json_tmp.creditCheck.creditScoreV2.addiProbabilityDefault.value as probability_default_addi,
    json_tmp.creditCheck.creditScoreV2.probabilityDefault.value as probability_default_bureau,
    coalesce(json_tmp.application.prospect.id.id,
             json_tmp.metadata.context.clientId) as client_id,
    json_tmp.creditCheck.storeFraudRiskLevel.value as store_fraud_risk_level
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw_modeling.creditcheckpassedwithlowerapprovedamount_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN to_date("2022-01-01") AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN to_timestamp("2022-01-01") AND to_timestamp("2022-01-30")
