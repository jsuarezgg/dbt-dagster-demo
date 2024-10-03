
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


--raw_modeling.loanrefinanceproposalscreatedco_co
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
        -- CUSTOM ATTRIBUTES
        EXPLODE(json_tmp.creditCheck.loanProposals) as loan_proposal
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'loanrefinanceproposalscreatedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}

)

SELECT
    CONCAT('EID_',event_id,'_LPEVID_',loan_proposal.id) AS surrogate_key,
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
    loan_proposal.id AS loan_proposal_id,
    loan_proposal.approved AS is_approved,
    loan_proposal.approvedAmount AS approved_amount,
    loan_proposal.creationDate AS creation_date,
    loan_proposal.effectiveAnnualRate AS interest_rate,
    loan_proposal.effectiveMonthlyRate AS effective_monthly_rate,
    loan_proposal.firstPaymentDate AS first_payment_date,
    --loan_proposal.guarantee.feeRate
    --loan_proposal.guarantee.feeRateTax
    loan_proposal.guarantee.rate AS fga_client_rate,
    --loan_proposal.guarantee.totalGuarantee
    loan_proposal.guarantee.totalGuaranteeRate AS total_fga_rate,
    --loan_proposal.installments._ARRAY_.dueDate
    --loan_proposal.installments._ARRAY_.interest
    --loan_proposal.installments._ARRAY_.pmt
    --loan_proposal.installments._ARRAY_.principal
    --loan_proposal.originationDate
    --loan_proposal.originationZone
    loan_proposal.term AS term,
    --loan_proposal.totalAmount
    --loan_proposal.totalInstallment
    loan_proposal.totalInterest AS total_interest,
    --loan_proposal.totalPrincipal
    loan_proposal.type AS loan_proposal_type
FROM select_explode