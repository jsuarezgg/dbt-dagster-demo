{{
    config(
        materialized='incremental',
        unique_key='application_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH FraudPoliciesBooleanDecisionUnit AS (
    SELECT
        context_id AS application_id,
        created_at,
        payload:decision.reason.model AS model,
        payload:decision.reason.score AS score,
        ROW_NUMBER() OVER(PARTITION BY context_id ORDER BY created_at DESC) AS rn
    FROM {{ ref('f_risk_service_decision_unit_logs_co') }} dul
    WHERE decision_unit = 'FraudPoliciesBooleanDecisionUnit' and (right(payload:decision.config,9)<>'_fallback' or payload:decision.config is null)
    {% if is_incremental() %}
        AND created_at::DATE BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
    {% endif %}
)

SELECT
    udw.application_id,
    COALESCE(dul.model, udw.fraud_model_version) AS fraud_model_version,
    COALESCE(dul.score, udw.fraud_model_score) AS fraud_model_score
FROM FraudPoliciesBooleanDecisionUnit dul
LEFT JOIN {{ ref('f_underwriting_fraud_stage_co') }} udw   ON udw.application_id = dul.application_id AND dul.rn = 1
