


--raw.prospect_br_fraud_rules_evaluation_results_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    application_id,
    created_at,
    evaluation_data,
    evaluation_result,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.prospect_br_fraud_rules_evaluation_results_br