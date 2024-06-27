{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_user_full_refresh_overwrite
SELECT
	Id AS user_id,
	Name AS user_name,
	Email AS user_email,
	Username AS user_username,
	IsActive AS user_is_active,
	UserType AS user_user_type,
	CONCAT('https://addi.lightning.force.com/lightning/r/User/',Id ,'/view') AS user_link,
	LastModifiedDate.member0 AS user_lastmoddate,
	SystemModstamp.member0 AS user_systemmodstamp,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'salesforce_user_full_refresh_overwrite') }}



