{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_opportunity_history_full_refresh_overwrite
SELECT
	Id AS opportunity_history_id,
	OpportunityId AS opportunity_id,
	Amount AS opportunity_history_amount,
	StageName AS opportunity_history_stage_name,
	CloseDate.member0 AS opportunity_history_close_date,
	CreatedDate.member0 AS opportunity_history_created_date,
	SystemModstamp.member0 AS opportunity_history_systemmodstamp,
	ExpectedRevenue AS opportunity_history_expected_revenue,
	ForecastCategory AS opportunity_history_forecast_category,
	CreatedById AS opportunity_history_created_by_id,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'salesforce_opportunity_history_full_refresh_overwrite') }}



