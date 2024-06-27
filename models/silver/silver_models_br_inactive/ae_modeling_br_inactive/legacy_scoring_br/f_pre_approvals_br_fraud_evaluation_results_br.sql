{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.risk_service_decision_unit_segments_br
SELECT
    {{ scoring_current_tables_fields() }}
-- DBT SOURCE REFERENCE
from {{ ref('risk_service_decision_unit_logs_br') }}
where 1=1
and decision_unit = 'FraudPoliciesBooleanDecisionUnit'
and flow = 'PREAPPROVALS_PAGO_BR'
