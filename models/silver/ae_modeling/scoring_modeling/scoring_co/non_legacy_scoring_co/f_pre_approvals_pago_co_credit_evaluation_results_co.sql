{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.prospect_co_fraud_rules_evaluation_results_co
SELECT
    {{ scoring_previous_tables_fields() }}
-- DBT SOURCE REFERENCE
FROM {{ ref('pre_approvals_pago_co_credit_evaluation_results_co') }}
