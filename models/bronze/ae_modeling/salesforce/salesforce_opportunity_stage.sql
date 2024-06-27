{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_opportunity_stage_full_refresh_overwrite
SELECT
	Id AS opportunity_stage_id,
	ApiName AS opportunity_stage_api_name,
	MasterLabel AS opportunity_stage_master_label,
	SortOrder AS opportunity_stage_sort_order,
	IsWon AS opportunity_stage_is_won,
	IsActive AS opportunity_stage_is_active,
	IsClosed AS opportunity_stage_is_closed,
	ForecastCategory AS opportunity_stage_forecast_category,
	DefaultProbability AS opportunity_stage_default_probability,
	CreatedDate.member0 AS opportunity_stage_created_date,
	LastModifiedDate.member0 AS opportunity_stage_lastmoddate,
	SystemModstamp.member0 AS opportunity_stage_systemmodstamp,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'salesforce_opportunity_stage_full_refresh_overwrite') }}



