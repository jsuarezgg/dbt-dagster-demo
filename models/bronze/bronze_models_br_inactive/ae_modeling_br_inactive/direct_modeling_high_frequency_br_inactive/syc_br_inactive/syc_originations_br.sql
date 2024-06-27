{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_originations_br
SELECT
    -- DIRECT MODELING FIELDS
    approved_amount,
    cancellation_reason,
    client_id,
    effective_annual_rate,
    first_payment_date,
    is_cancelled,
    is_fully_paid_in_old_loan_tape,
    loan_id,
    origination_date,
    origination_zone,
    term,
    updates,
    ownership,
    low_balance_loan,
    guarantee_coverage,
    guarantee_fee_rate,
    guarantee_tax_rate,
    creation_date,
    iof_daily_rate,
    iof_additional_rate,
    iof_installment_limit_rate,
    iof_amount,
    config,
    product_type,
    iof_is_financed,
    preapproval_amount,
    initial_addicupo,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_originations_br') }}