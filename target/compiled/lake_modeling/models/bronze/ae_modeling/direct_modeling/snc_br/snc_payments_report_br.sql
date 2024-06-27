


SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    index,
    client_id,
    delinquency_iof,
    moratory_interest,
    interest_overdue,
    principal_overdue,
    guarantee_overdue,
    current_interest,
    current_principal,
    current_guarantee,
    unpaid_guarantee,
    unpaid_principal,
    prepayment_benefit,
    date,
    payment_id,
    ownership,
    channel,
    loan_id,
    leftover,
    late_fees,
    total_payment,
    created_at,

    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at

-- DBT SOURCE REFERENCE
FROM raw.snc_payments_report_br