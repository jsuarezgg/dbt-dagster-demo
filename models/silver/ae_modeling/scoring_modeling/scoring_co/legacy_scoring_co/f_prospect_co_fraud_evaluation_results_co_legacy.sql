{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.prospect_co_fraud_evaluation_results_co
SELECT
    {{ scoring_previous_tables_fields() }}
-- DBT SOURCE REFERENCE
FROM {{ ref('prospect_co_fraud_evaluation_results_co') }}
