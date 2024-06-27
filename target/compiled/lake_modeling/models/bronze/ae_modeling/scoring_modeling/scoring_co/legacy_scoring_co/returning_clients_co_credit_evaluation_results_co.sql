


--raw.returning_clients_co_credit_evaluation_results_co
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
FROM raw.returning_clients_co_credit_evaluation_results_co