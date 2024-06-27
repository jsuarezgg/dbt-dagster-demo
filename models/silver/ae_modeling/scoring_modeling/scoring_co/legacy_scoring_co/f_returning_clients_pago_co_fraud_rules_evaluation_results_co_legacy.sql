{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.returning_clients_pago_co_fraud_rules_evaluation_results_co
SELECT
    {{ scoring_previous_tables_fields() }}
-- DBT SOURCE REFERENCE
FROM {{ ref('returning_clients_pago_co_fraud_rules_evaluation_results_co') }}
