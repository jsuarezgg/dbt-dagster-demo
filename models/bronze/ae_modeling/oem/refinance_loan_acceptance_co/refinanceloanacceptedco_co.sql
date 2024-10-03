
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


--raw_modeling.refinanceloanacceptedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.loan.id AS loan_id,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.application.product AS product,
    json_tmp.ally.store.slug AS store_slug,
    json_tmp.user.id AS store_user_id,
    json_tmp.client.nationalIdentification.type AS id_type,
    json_tmp.client.nationalIdentification.number AS id_number,
    json_tmp.creditCheck.productV2 AS product_v2,
    json_tmp.creditCheck.evaluationType AS evaluation_type,
    json_tmp.creditCheck.status as credit_status,
    json_tmp.creditCheck.statusReason as credit_status_reason,
    json_tmp.creditCheck.totalADDICupo as client_max_exposure,
    json_tmp.creditCheck.creditPolicy.name as credit_policy_name,
    --json_tmp.loan.acceptance.message
    --json_tmp.loan.acceptance.otp
    --json_tmp.loan.approved
    json_tmp.loan.approvedAmount AS approved_amount,
    --json_tmp.loan.creationDate
    json_tmp.loan.effectiveAnnualRate AS effective_annual_rate,
    --json_tmp.loan.effectiveMonthlyRate
    TO_TIMESTAMP(json_tmp.loan.firstPaymentDate) AS first_payment_date,
    --json_tmp.loan.guarantee.feeRate AS guarantee_fee_rate,
    --json_tmp.loan.guarantee.feeRateTax AS guarantee_fee_rate_tax,
    --json_tmp.loan.guarantee.rate AS guarantee_fee_rate_simple, -- ??? it's like a limited guarante rate
    json_tmp.loan.guarantee.totalGuarantee AS guarantee_total,
    json_tmp.loan.guarantee.totalGuaranteeRate AS guarantee_rate,
    json_tmp.loan.isLowBalance AS lbl,
    TO_TIMESTAMP(json_tmp.loan.originationDate) AS origination_date,
    --json_tmp.loan.originationZone
    json_tmp.loan.term AS term,
    --json_tmp.loan.totalAmount
    --json_tmp.loan.totalInstallment
    json_tmp.loan.totalInterest AS total_interest,
    --json_tmp.loan.totalPrincipal
    json_tmp.loan.type AS loan_type

    -- CAST(ocurred_on AS TIMESTAMP) AS loanacceptedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'refinanceloanacceptedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
