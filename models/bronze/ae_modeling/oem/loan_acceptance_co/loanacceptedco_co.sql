
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.loanacceptedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
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
    json_tmp.creditCheck.productV2 AS product_v2,
    json_tmp.creditCheck.evaluationType AS evaluation_type,
    json_tmp.creditCheck.learningPopulation as learning_population,
    json_tmp.creditCheck.totalADDICupo as client_max_exposure,
    json_tmp.creditCheck.income.netValue as credit_check_income_net_value,
    json_tmp.creditCheck.income.provider as credit_check_income_provider,
    json_tmp.creditCheck.income.type as credit_check_income_type,
    coalesce(json_tmp.creditCheck.lowBalanceLoanV2,
             json_tmp.creditCheck.lowBalanceLoan) AS lbl,
    -- json_tmp.creditCheck.income.validator.calculationMethod as credit_check_income_validator_calculation_method,
    -- json_tmp.creditCheck.income.validator.contributionType as credit_check_income_validator_contribution_type,
    json_tmp.creditCheck.creditPolicy.name as credit_policy_name,
        coalesce(json_tmp.creditCheck.scoreV2.name,
                json_tmp.creditCheck.score.name) as credit_score_name,
        coalesce(json_tmp.creditCheck.scoreV2.value,
                json_tmp.creditCheck.score.value) as credit_score,
    -- json_tmp.creditScoreV2.name.value as credit_score_product,
    json_tmp.creditCheck.status as credit_status,
    json_tmp.creditCheck.statusReason as credit_status_reason,
    json_tmp.creditCheck.fraudModel.score as fraud_model_score,
    json_tmp.creditCheck.fraudModel.version as fraud_model_version,
    json_tmp.creditCheck.groupName as group_name,
    -- json_tmp.psychometricAssessment.evaluation.overallScore as ia_overall_score,
    coalesce(json_tmp.creditCheck.scoreV2.pdCalculationMethod,
             json_tmp.creditCheck.score.pdCalculationMethod) as pd_calculation_method,
    coalesce(json_tmp.creditCheck.scoreV2.addiProbabilityDefault,
             json_tmp.creditCheck.score.addiProbabilityDefault) as probability_default_addi,
    coalesce(json_tmp.creditCheck.scoreV2.probabilityDefault,
             json_tmp.creditCheck.score.probabilityDefault) as probability_default_bureau,
    json_tmp.creditCheck.fraudModel.riskLevel as store_fraud_risk_level,
    json_tmp.creditCheck.totalDebtToServiceRatio as tdsr,
    json_tmp.loan.approvedAmount AS approved_amount,
    json_tmp.loan.guarantee.totalGuaranteeRate AS guarantee_rate,
    json_tmp.loan.guarantee.totalGuarantee AS guarantee_amount,
    json_tmp.loan.guarantee.provider as guarantee_provider,
    json_tmp.loan.effectiveAnnualRate AS effective_annual_rate,
    json_tmp.loan.id AS loan_id,
    json_tmp.loan.type AS loan_type,
    CAST(json_tmp.loan.originationDate AS timestamp) AS origination_date,
    json_tmp.ally.store.slug AS store_slug,
    json_tmp.user.id AS store_user_id,
    CAST(json_tmp.loan.firstPaymentDate AS timestamp) AS first_payment_date,
    json_tmp.client.nationalIdentification.type AS id_type,
    json_tmp.client.nationalIdentification.number AS id_number,
    json_tmp.loan.downPaymentAmount AS down_payment_amount,
    json_tmp.loan.term,
    json_tmp.client.name.full AS full_name,
    json_tmp.client.nationality AS client_nationality,
    json_tmp.client.email.address AS application_email,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(1 AS TINYINT) AS custom_loan_acceptance_passed,
    json_tmp.loan.acceptance.otp AS otp_code,
    named_struct(
        'event_id', json_tmp.eventId,
        'event_type', json_tmp.originationEventType,
        'ocurred_on', ocurred_on::TIMESTAMP,
        'acceptance',
            named_struct(
            "otp", json_tmp.loan.acceptance.otp
            )
    ) AS loan_acceptance_detail_json,
    {{ dbt_utils.surrogate_key(['event_id', 'json_tmp.loan.id']) }} AS surrogate_key
    -- CAST(ocurred_on AS TIMESTAMP) AS loanacceptedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'loanacceptedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
