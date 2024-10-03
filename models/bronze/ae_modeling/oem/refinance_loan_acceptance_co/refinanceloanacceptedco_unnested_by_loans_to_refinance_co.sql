
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.refinanceloanacceptedco_co
WITH explode_items as (
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
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.loan.id AS refinanced_by_origination_of_loan_id,
    explode(json_tmp.application.loansToRefinance) AS loan_to_refinance
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS refinanceloanacceptedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'refinanceloanacceptedco_co') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)


SELECT 
    CONCAT('EID_',event_id,'_LTRID_',loan_to_refinance.id.value) AS surrogate_key,
    MD5(CONCAT(application_id,loan_to_refinance.id.value)) AS custom_loan_refinance_id, -- capture compound key: single row for every app+loan_to_refinance_loan_id combination
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	application_id,
	client_id,
	ally_slug,
	refinanced_by_origination_of_loan_id,
    loan_to_refinance.id.value AS loan_id,
    loan_to_refinance.isEligibleForRefinance AS is_eligible_for_refinance,
    --loan_to_refinance.originationDate AS loan_origination_date, -- EXCLUDED - This value should come from our BNPL tables, not from here
    loan_to_refinance.outstandingBalance.total AS outstanding_balance_total,
    loan_to_refinance.outstandingBalance.unpaidPrincipal AS outstanding_balance_unpaid_principal,
    loan_to_refinance.outstandingBalance.interestOverdue AS outstanding_balance_interest_overdue,
    loan_to_refinance.outstandingBalance.lateFees AS outstanding_balance_collection_fees, -- Renamed due to legal concerns
    loan_to_refinance.outstandingBalance.moratoryInterest AS outstanding_balance_moratory_interest,
    loan_to_refinance.paidValues.totalGuaranteePaid AS paid_guarantee_at_refinance_application,
    loan_to_refinance.paidValues.totalInterestPaid AS paid_interest_at_refinance_application,
    loan_to_refinance.paidValues.totalLateFeesPaid AS paid_collection_fees_at_refinance_application, -- Renamed due to legal concerns
    loan_to_refinance.paidValues.totalPrincipalPaid AS paid_principal_at_refinance_application,
    loan_to_refinance.paidValues.totalPaymentApplied AS total_payment_applied_at_refinance_application,
    loan_to_refinance.totalPrepaymentBenefit AS total_prepayment_benefit_at_refinance_application,
    --loan_to_refinance.currentInstallment AS loan_current_installment, -- EXCLUDED - This value for Loan Tape Service has never been trustworthy
    --loan_to_refinance.remainingInstallments AS loan_remaining_installments, -- EXCLUDED - This value for Loan Tape Service has never been trustworthy
    loan_to_refinance.unpaidGuarantee AS unpaid_guarantee_at_refinance_application
    --- END OF ORDER REFACTOR
FROM explode_items