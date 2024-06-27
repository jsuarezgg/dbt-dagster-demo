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
--raw.dynamic_crm_aggregator_clients_co
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
    _ingestion_at AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'dynamic_crm_aggregator_clients_co') }}
WHERE NULLIF(kustomer_id,'') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY kustomer_id ORDER BY COALESCE(financial_info_updated_at,aggregator_created_at)) = 1