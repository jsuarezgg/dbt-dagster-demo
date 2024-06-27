{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_ally_activation_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
	slug AS ally_slug,
	status,
	data:externalInfo.experiments AS external_info_experiments,
	data:externalInfo.additionalInformation.finalBeneficiaries AS external_info_final_beneficiaries,
	data:externalInfo.activationExternalSource AS external_info_activation_external_source,
	data:externalSource AS external_source,
	data:externalSourceId AS external_source_id,
	data:externalInfo.verticalName AS external_info_vertical_name,
	data:externalInfo.additionalInformation.averageSales  AS external_info_average_sales,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_ally_activation_co') }}
