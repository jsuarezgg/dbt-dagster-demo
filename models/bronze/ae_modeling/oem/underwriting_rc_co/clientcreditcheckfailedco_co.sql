
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


--raw_modeling.clientcreditcheckfailedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
      -- Fill with your mapped fields
    json_tmp.metadata.context.allyId as ally_slug,
    json_tmp.application.id as application_id,
    json_tmp.creditCheck.bypassRejectedReason AS bypassed_reason,
    json_tmp.client.id as client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    COALESCE(json_tmp.application.channel,json_tmp.metadata.context.channel) AS channel,
    json_tmp.application.product AS product,
    coalesce(cast(json_tmp.creditCheck.lowBalanceLoan as boolean),
             cast(json_tmp.creditCheck.lowBalanceLoanV2 as boolean)) as lbl,
    CAST(json_tmp.creditCheck.learningPopulation AS BOOLEAN) as learning_population,
    json_tmp.creditCheck.totalADDICupo AS client_max_exposure,
    json_tmp.creditCheck.income.netValue AS credit_check_income_net_value,
    json_tmp.creditCheck.income.provider AS credit_check_income_provider,
    json_tmp.creditCheck.income.type AS credit_check_income_type,
    json_tmp.creditCheck.creditPolicy.name AS credit_policy_name,
    coalesce(json_tmp.creditCheck.scoreV2.value,json_tmp.creditCheck.score.value) AS credit_score, 
    coalesce(json_tmp.creditCheck.scoreV2.name,json_tmp.creditCheck.score.name) AS credit_score_name,
    json_tmp.creditCheck.status AS credit_status,
    json_tmp.creditCheck.statusReason AS credit_status_reason,
    json_tmp.creditCheck.fraudModel.score AS fraud_model_score,
    json_tmp.creditCheck.fraudModel.version AS fraud_model_version,
    json_tmp.creditCheck.groupName AS group_name,
    json_tmp.creditCheck.loanProposals.effectiveAnnualRate AS interest_rate,
    coalesce(json_tmp.creditCheck.scoreV2.pdCalculationMethod,json_tmp.creditCheck.score.pdCalculationMethod) AS pd_calculation_method,
    json_tmp.creditCheck.fraudModel.policy AS policy,
    coalesce(json_tmp.creditCheck.scoreV2.addiProbabilityDefault,json_tmp.creditCheck.score.addiProbabilityDefault) AS probability_default_addi,
    coalesce(json_tmp.creditCheck.scoreV2.probabilityDefault,json_tmp.creditCheck.score.probabilityDefault) AS probability_default_bureau,
    json_tmp.application.requestedAmount AS requested_amount,
    cast(json_tmp.creditCheck.shouldBeBlackListed as boolean) AS should_be_black_listed,
    json_tmp.creditCheck.fraudModel.riskLevel AS store_fraud_risk_level,
    json_tmp.creditCheck.totalDebtToServiceRatio AS tdsr,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(1 AS TINYINT) AS credit_check_failed,
    cast(true as BOOLEAN) as returning_client,
    cast(false AS BOOLEAN) AS originated_loan
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'clientcreditcheckfailedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
