
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


--raw_modeling.preapprovalcreditcheckpassedbr_br
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
    json_tmp.creditCheck.totalADDICupo as client_max_exposure,
    json_tmp.creditCheck.income.netValue as credit_check_income_net_value,
    json_tmp.creditCheck.income.provider as credit_check_income_provider,
    json_tmp.creditCheck.income.type as credit_check_income_type,
    /* json_tmp.creditCheck.income.validator.calculationMethod as credit_check_income_validator_calculation_method,
    json_tmp.creditCheck.income.validator.contributionType as credit_check_income_validator_contribution_type, */ -- validator is a string
    json_tmp.creditCheck.creditPolicy.name as credit_policy_name,
    json_tmp.creditCheck.score.name as credit_score_name,
    --json_tmp.creditCheck.scoreV2 as credit_score_product,
    json_tmp.creditCheck.status as credit_status,
    json_tmp.creditCheck.statusReason as credit_status_reason,
    json_tmp.creditCheck.fraudModel.score as fraud_model_score,
    json_tmp.creditCheck.fraudModel.version as fraud_model_version,
    json_tmp.creditCheck.groupName as group_name,
    -- json_tmp.psychometricAssessment.evaluation.overallScore as ia_overall_score,
    json_tmp.creditCheck.score.pdCalculationMethod as pd_calculation_method,
    json_tmp.creditCheck.score.addiProbabilityDefault as probability_default_addi,
    json_tmp.creditCheck.score.probabilityDefault as probability_default_bureau,
    json_tmp.creditCheck.fraudModel.riskLevel as store_fraud_risk_level,
    json_tmp.creditCheck.totalDebtToServiceRatio as tdsr,
    json_tmp.creditCheck.score.value as credit_score,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(false AS BOOLEAN) as returning_client
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'preapprovalcreditcheckpassedbr_br') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
