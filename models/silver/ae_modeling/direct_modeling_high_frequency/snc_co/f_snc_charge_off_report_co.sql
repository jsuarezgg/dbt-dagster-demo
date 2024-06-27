{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.snc_charge_off_report_co
SELECT
    loan_id,
    client_id,
    product_type,
    threshold_days,
    last_days_past_due,
    max_days_past_due,
    calculation_date,
    origination_date,
    first_due_date,
    charge_off_date,
    is_charge_off,
    charge_off_upb,
    charge_off_total,
    recovery_upb,
    recovery_total,
    recovery_fga,
    refinanced,
    condonation_upb_apart_from_fga,
    condonation_total_apart_from_fga,
    created_at,
    days_past_due_at_chargeoff,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('snc_charge_off_report_co') }}
