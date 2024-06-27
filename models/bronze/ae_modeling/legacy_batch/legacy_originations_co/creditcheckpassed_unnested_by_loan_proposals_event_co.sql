
{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.creditcheckpassed_co
WITH select_explode AS (
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
        coalesce(json_tmp.loanApplication.allyId.slug,
                json_tmp.application.allyId.slug,
                json_tmp.metadata.context.allyId) as ally_slug,
        coalesce(json_tmp.loanApplication.applicationId.id,
                json_tmp.application.applicationId.id,
                json_tmp.application.applicationId.value,
                json_tmp.metadata.context.traceId) as application_id,
        coalesce(json_tmp.loanApplication.prospect.id.id,
                json_tmp.application.prospect.id.id,
                json_tmp.metadata.context.clientId) as client_id,
        json_tmp.lowBalanceLoanV2.value as lbl,
        json_tmp.learningPopulation.value as learning_population,
        json_tmp.returningClient as returning_client,
        EXPLODE(json_tmp.loanProposals) as loan_proposals_event,
        -- CUSTOM ATTRIBUTES
        'V1' as custom_idv_version
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'creditcheckpassed_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}

)

SELECT 
    CONCAT('EID_',event_id,'_LPEVID_',loan_proposals_event.id.id) AS surrogate_key,
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	ally_slug,
    application_id,
	client_id,
    lbl,
    coalesce(learning_population, loan_proposals_event.learningPopulation) as learning_population,
    returning_client,
	loan_proposals_event.allyDiscountFee as ally_mdf,
    loan_proposals_event.approvedAmount as approved_amount,
    loan_proposals_event.contributionMargin as contribution_margin,
    loan_proposals_event.decisionNPV as decision_npv,
    loan_proposals_event.decisionDiscountRate as discount_rate,
    loan_proposals_event.guaranteePolicy.rate.value as fga_client_rate,
    loan_proposals_event.guaranteePolicy.feeRate.value as fga_comission_rate,
    loan_proposals_event.guaranteePolicy.feeRateTax.value as fga_tax_rate,
    loan_proposals_event.firstLoanCash.value as first_loan_cash,
    loan_proposals_event.reportingNPV as first_loan_npv,
    loan_proposals_event.firstLoanROE.value as first_loan_roe,
    loan_proposals_event.effectiveAnnualRate as interest_rate,
    loan_proposals_event.lifetimeCash.value as lifetime_cash,
    loan_proposals_event.repeatingNPV as lifetime_npv,
    loan_proposals_event.lifetimeROE.value as lifetime_roe,
    loan_proposals_event.id.id as loan_proposal_id,
    loan_proposals_event.term as term,
    loan_proposals_event.guaranteePolicy.totalRate.value as total_fga_rate,
    loan_proposals_event.totalInterest as total_interest
FROM select_explode