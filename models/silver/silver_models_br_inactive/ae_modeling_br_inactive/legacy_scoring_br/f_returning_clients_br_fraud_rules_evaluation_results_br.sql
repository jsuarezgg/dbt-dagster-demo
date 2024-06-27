{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.returning_clients_br_fraud_rules_evaluation_results_br
SELECT
    {{ scoring_previous_tables_fields() }}
-- DBT SOURCE REFERENCE
FROM {{ ref('returning_clients_br_fraud_rules_evaluation_results_br') }}
