


--raw_modeling.creditcheckfailed_br
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
    json_tmp.metadata.context.traceId as application_id,
    json_tmp.totalAddiCupo as client_max_exposure,
    json_tmp.income.netValue.value as credit_check_income_net_value,
    json_tmp.income.provider.value as credit_check_income_provider,
    json_tmp.income.type.value as credit_check_income_type,
    /*coalesce(json_tmp.income.validator.calculationMethod.value,
             json_tmp.income.validator.calculationMethod) as credit_check_income_validator_calculation_method,*/
    -- json_tmp.income.validator.contributionType.value as credit_check_income_validator_contribution_type,
    json_tmp.creditPolicyName as credit_policy_name,
    json_tmp.creditScoreName as credit_score_name,
    json_tmp.creditScoreV2.name.value as credit_score_product,
    json_tmp.status as credit_status,
    json_tmp.statusReason as credit_status_reason,
    json_tmp.fraudModelScore as fraud_model_score,
    json_tmp.fraudModelVersion as fraud_model_version,
    json_tmp.groupName as group_name,
    json_tmp.creditScoreV2.pdCalculationMethod as pd_calculation_method,
    json_tmp.creditScoreV2.addiProbabilityDefault.value as probability_default_addi,
    json_tmp.creditScoreV2.probabilityDefault.value as probability_default_bureau,
    json_tmp.metadata.context.clientId as client_id,
    json_tmp.storeFraudRiskLevel as store_fraud_risk_level,
    json_tmp.totalDebtToServiceRatio.value as tdsr,
    COALESCE(json_tmp.creditScore, json_tmp.creditScoreV2.value.value) as credit_score
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw_modeling.creditcheckfailed_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN to_date("2022-01-01") AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN to_timestamp("2022-01-01") AND to_timestamp("2022-01-30")
