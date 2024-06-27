{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_account_full_refresh_overwrite
SELECT
	Id AS account_id,
	Name AS account_name,
	KAM__c AS account_kam_user_id,
    Hunter__c AS account_hunter_user_id,
	POD__c AS account_pod,
	Vertical__c AS account_vertical,
	CONCAT('https://addi.lightning.force.com/lightning/r/Account/',Id ,'/view') AS account_link,
	LastModifiedDate.member0 AS account_lastmoddate,
	SystemModstamp.member0 AS account_systemmodstamp,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'salesforce_account_full_refresh_overwrite') }}



