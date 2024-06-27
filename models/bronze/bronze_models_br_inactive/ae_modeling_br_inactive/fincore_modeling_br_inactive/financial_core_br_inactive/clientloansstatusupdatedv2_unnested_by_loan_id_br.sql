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


--raw_modeling.clientloansstatusupdatedv2_unnested_by_loan_id_br
WITH select_explode AS (

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
        COALESCE(json_tmp.client.clientId.value, json_tmp.metadata.context.clientId) AS client_id,
        EXPLODE(json_tmp.client.loans.loans) AS loan
        -- CUSTOM ATTRIBUTES
        -- Fill with your custom attributes
    -- DBT SOURCE REFERENCE
    FROM {{ source('raw_modeling', 'clientloansstatusupdatedv2_br') }}
    -- DBT INCREMENTAL SENTENCE

    {% if is_incremental() %}
        WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
    {% endif %}

)

SELECT 
    {{ dbt_utils.surrogate_key(['event_id', 'loan.loanId.value']) }} AS surrogate_key,
    event_name_original,
    event_name,
    event_id,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    client_id,
    loan.loanId.value AS loan_id,
    CAST(loan.originationDate.value AS timestamp) AS origination_date,
    loan.term.value AS term,
    loan.status.daysPastDue.value AS days_past_due,
    loan.status.paidInstallments.value AS paid_installments,
    loan.effectiveAnnualRate.value AS effective_annual_rate,
    loan.approvedAmount.value AS approved_amount,
    loan.status.totalPrincipalPaid.value AS total_principal_paid,
    loan.status.minPayment.value AS min_payment,
    loan.status.fullPayment.value AS full_payment,
    loan.status.delinquencyBalance.value AS delinquency_balance,
    loan.status.principalOverdue.value AS principal_overdue,
    loan.status.interestOverdue.value AS interest_overdue,
    loan.status.guaranteeOverdue.value AS guarantee_overdue,
    loan.status.unpaidPrincipal.value AS unpaid_principal,
    loan.status.interestOnOverduePrincipal.value AS interest_on_overdue_principal,
    loan.status.initialInstallmentAmount.value AS initial_installment_amount,
    loan.status.currentInstallmentAmount.value AS current_installment_amount,
    loan.status.unpaidGuarantee.value AS unpaid_guarantee,
    loan.status.totalPaymentApplied.value AS total_payment_applied,
    loan.state AS state,
    loan.loanOwnership AS ownership,
    CAST(loan.firstPaymentDate.value AS timestamp) AS first_payment_date,
    loan.status.monthsOnBook.value AS months_on_book,
    loan.status.applicableRate.value AS applicable_rate,
    loan.status.totalInterestPaid.value AS total_interest_paid,
    loan.status.isFullyPaid.value AS is_fully_paid,
    loan.status.lateFees AS late_fees,
    loan.status.totalLateFeesPaid AS total_late_fees_paid,
    loan.status.totalLateFeesCondoned AS total_late_fees_condoned,
    CAST(TRUE AS BOOLEAN) AS custom_fincore_included

FROM select_explode
