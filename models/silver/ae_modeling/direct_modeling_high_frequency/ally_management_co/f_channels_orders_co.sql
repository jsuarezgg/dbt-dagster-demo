{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT
    -- MANDATORY FIELDS
	application_id,
	ally_slug,
    data:['sale']['seller']['nationalIdentification']['type'] AS seller_id_type,
	data:['sale']['seller']['nationalIdentification']['number'] AS seller_id_number,
    ingested_at,
    updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM {{ ref('channels_orders_co') }} AS credit_contracts
