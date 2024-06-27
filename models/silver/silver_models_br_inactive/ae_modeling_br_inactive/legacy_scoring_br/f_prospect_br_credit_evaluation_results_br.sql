{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.offers_service_offers_logs_br
SELECT
    -- DIRECT MODELING FIELDS
	decision_id,
	context_id,
    user_id,
	created_at,
	flow,
	payload,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('offers_service_offers_logs_br') }}
where 1=1
AND flow = 'PROSPECTS_PAGO_BR'