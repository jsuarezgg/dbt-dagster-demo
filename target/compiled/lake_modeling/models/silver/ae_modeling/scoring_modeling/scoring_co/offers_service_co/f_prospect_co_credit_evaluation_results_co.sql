


--bronze.offers_service_offers_logs_co
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
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.offers_service_offers_logs_co
where 1=1
AND flow = 'PROSPECTS_FINANCIA_CO'