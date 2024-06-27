


--raw_modeling.conditionalcreditcheckpassed_co
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
    coalesce(json_tmp.loanApplication.allyId.slug,
             json_tmp.application.allyId.slug,
             json_tmp.metadata.context.allyId) as ally_slug,
    coalesce(json_tmp.loanApplication.applicationId.id,
             json_tmp.application.applicationId.id,
             json_tmp.application.applicationId.value,
             json_tmp.creditCheck.applicationId.id,
             json_tmp.creditCheck.applicationId.value,
             json_tmp.metadata.context.traceId) as application_id,
    coalesce(json_tmp.loanApplication.prospect.id.id,
             json_tmp.application.prospect.id.id,
             json_tmp.creditCheck.prospectId.value,
             json_tmp.metadata.context.clientId) as client_id,
    json_tmp.creditCheck.lowBalanceLoanV2.value as lbl,
    json_tmp.creditCheck.learningPopulation.value as learning_population,
    json_tmp.creditCheck.returningClient as returning_client,
    json_tmp.creditCheck.totalAddiCupo as client_max_exposure,
    json_tmp.creditCheck.income.netValue.value as credit_check_income_net_value,
    json_tmp.creditCheck.income.provider.value as credit_check_income_provider,
    json_tmp.creditCheck.income.type.value as credit_check_income_type,
    json_tmp.creditCheck.income.validator.calculationMethod.value as credit_check_income_validator_calculation_method,
    json_tmp.creditCheck.income.validator.contributionType.value as credit_check_income_validator_contribution_type,
    json_tmp.creditCheck.creditPolicyName as credit_policy_name,
    coalesce(json_tmp.creditCheck.creditScore,
             json_tmp.creditCheck.creditScoreV2.value.value) as credit_score,
    json_tmp.creditCheck.creditScoreName as credit_score_name,
    json_tmp.creditCheck.creditScoreV2.name.value as credit_score_product,
    json_tmp.creditCheck.status as credit_status,
    json_tmp.creditCheck.statusReason as credit_status_reason,
    json_tmp.creditCheck.fraudModelScore as fraud_model_score,
    json_tmp.creditCheck.fraudModelVersion as fraud_model_version,
    json_tmp.creditCheck.groupName as group_name,
    json_tmp.creditCheck.creditScoreV2.pdCalculationMethod as pd_calculation_method,
    json_tmp.creditCheck.creditScoreV2.addiProbabilityDefault.value as probability_default_addi,
    json_tmp.creditCheck.creditScoreV2.probabilityDefault.value as probability_default_bureau,
    json_tmp.creditCheck.storeFraudRiskLevel as store_fraud_risk_level,
    json_tmp.creditCheck.totalDebtToServiceRatio.value as tdsr,
    json_tmp.creditCheck.bypassRejectedReason as bypassed_reason,
    json_tmp.creditCheck.shouldSkipIdv as should_skip_idv,
    -- CUSTOM ATTRIBUTES
    'V1' as custom_idv_version
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw_modeling.conditionalcreditcheckpassed_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
