{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.risk_service_decision_unit_logs_co
SELECT
    {{ scoring_current_tables_fields() }}
-- DBT SOURCE REFERENCE
from {{ ref('risk_service_decision_unit_logs_co') }}
where 1=1
and decision_unit = 'FraudPoliciesBooleanDecisionUnit'
and flow = 'PROSPECTS_FINANCIA_CO'
