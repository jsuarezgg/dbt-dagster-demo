
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


--raw_modeling.creditcheckfailedco_co
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
        json_tmp.metadata.context.allyId as ally_slug,
        json_tmp.application.id as application_id,
        json_tmp.client.id as client_id,
        coalesce(cast(json_tmp.creditCheck.lowBalanceLoan as boolean),
                cast(json_tmp.creditCheck.lowBalanceLoanV2 as boolean)) as lbl,
        json_tmp.creditCheck.learningPopulation as learning_population,
        -- CUSTOM ATTRIBUTES
        cast(false as BOOLEAN) as returning_client,
        EXPLODE(json_tmp.creditCheck.loanProposals) as loan_proposals_event
    -- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'creditcheckfailedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}

)

SELECT 
    CONCAT('EID_',event_id,'_LPEVID_',loan_proposals_event.id) AS surrogate_key,
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
    learning_population,
    returning_client,
	coalesce(loan_proposals_event.merchantDiscountFee.origination,
             loan_proposals_event.allyDiscountFee) as ally_mdf,
    loan_proposals_event.merchantDiscountFee.cancellation as ally_mdf_cancellation,
    loan_proposals_event.merchantDiscountFee.fraud as ally_mdf_fraud,
    loan_proposals_event.approvedAmount as approved_amount,
    loan_proposals_event.contributionMargin as contribution_margin,
    loan_proposals_event.npv.decisionNPV as decision_npv,
    loan_proposals_event.decisionDiscountRate as discount_rate,
    loan_proposals_event.guarantee.rate as fga_client_rate,
    loan_proposals_event.guarantee.feeRate as fga_comission_rate,
    loan_proposals_event.guarantee.feeRateTax as fga_tax_rate,
    loan_proposals_event.firstLoanCash as first_loan_cash,
    loan_proposals_event.npv.reportingNPV as first_loan_npv,
    loan_proposals_event.firstLoanROE as first_loan_roe,
    loan_proposals_event.npv.psychometricAssessmentPdMultiplier as ia_pd_multiplier,
    loan_proposals_event.effectiveAnnualRate as interest_rate,
    loan_proposals_event.lifetimeCash as lifetime_cash,
    loan_proposals_event.npv.repeatingNPV as lifetime_npv,
    loan_proposals_event.lifetimeROE as lifetime_roe,
    loan_proposals_event.id as loan_proposal_id,
    loan_proposals_event.term as term,
    loan_proposals_event.guarantee.totalGuaranteeRate as total_fga_rate,
    loan_proposals_event.totalInterest as total_interest
FROM select_explode