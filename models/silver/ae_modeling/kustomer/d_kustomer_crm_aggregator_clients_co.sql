{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 6511319 by 2023-09-27
--bronze.kustomer_crm_aggregator_clients_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
	kustomer_id,
	client_id,
	cell_phone_number,
    email,
	id_number,
	last_name,
	full_name,
	synchronized,
	sync_error_detail,
	aggregator_created_at,
	financial_info_updated_at,
    -- CUSTOM ATTRIBUTES
    ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_crm_aggregator_clients_co') }}
