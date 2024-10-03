{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_originations_co
SELECT DISTINCT
    -- DIRECT MODELING FIELDS
    approved_amount,
    cancellation_reason,
    so.client_id as client_id,
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
    CASE WHEN scms.client_id is null then 'lms'
                                     else 'kordev'
                                     end as loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('syc_originations_co') }} so
LEFT JOIN {{ ref('syc_client_migration_segments_co') }} scms
ON so.client_id  = scms.client_id