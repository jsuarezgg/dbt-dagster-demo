
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.clientcreditcheckpassed_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
      -- Fill with your mapped fields
    coalesce(json_tmp.clientLoanApplication.allyId.slug,
             json_tmp.metadata.context.allyId) as ally_slug,
    coalesce(json_tmp.clientLoanApplication.applicationId.id,
             json_tmp.clientLoanApplication.applicationId.value,
             json_tmp.metadata.context.traceId) as application_id,
    coalesce(json_tmp.clientLoanApplication.client.id.id,
             json_tmp.metadata.context.clientId) as client_id,
    json_tmp.lowBalanceLoanV2.value as lbl,
    json_tmp.loanProposals.learningPopulation as learning_population,
    json_tmp.returningClient as returning_client,
    json_tmp.totalAddiCupo as client_max_exposure,
    json_tmp.income.netValue.value as credit_check_income_net_value,
    json_tmp.income.provider.value as credit_check_income_provider,
    json_tmp.income.type.value as credit_check_income_type,
    json_tmp.income.validator.calculationMethod as credit_check_income_validator_calculation_method,
    json_tmp.income.validator.contributionType.value as credit_check_income_validator_contribution_type,
    json_tmp.creditPolicyName as credit_policy_name,
    coalesce(json_tmp.creditScore,
             json_tmp.creditScoreV2.value.value) as credit_score,
    json_tmp.creditScoreName as credit_score_name,
    json_tmp.creditScoreV2.name.value as credit_score_product,
    json_tmp.status as credit_status,
    json_tmp.statusReason as credit_status_reason,
    json_tmp.fraudModelScore.value as fraud_model_score,
    json_tmp.fraudModelVersion.value as fraud_model_version,
    json_tmp.groupName as group_name,
    json_tmp.creditScoreV2.pdCalculationMethod as pd_calculation_method,
    json_tmp.creditScoreV2.addiProbabilityDefault.value as probability_default_addi,
    json_tmp.creditScoreV2.probabilityDefault.value as probability_default_bureau,
    json_tmp.storeFraudRiskLevel.value as store_fraud_risk_level,
    json_tmp.totalDebtToServiceRatio.value as tdsr,
    -- CUSTOM ATTRIBUTES
    'V1' as custom_idv_version
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'clientcreditcheckpassed_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
