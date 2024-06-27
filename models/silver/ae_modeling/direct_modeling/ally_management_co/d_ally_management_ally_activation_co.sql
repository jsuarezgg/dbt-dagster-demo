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
    ally_slug,
	status,
    external_info_experiments,
    external_info_final_beneficiaries,
    external_info_activation_external_source,
    external_source,
    external_source_id,
    external_info_vertical_name,
    external_info_average_sales,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_activation_co') }}
