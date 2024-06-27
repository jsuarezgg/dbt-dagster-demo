{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH addresses_struct AS (
	SELECT
	application_id,
	NAMED_STRUCT('address', bureau_address_street) AS bureau_address_street_struct
	FROM {{ ref('f_kyc_bureau_contact_info_addresses_br_logs') }}
	ORDER BY application_id, bureau_address_order
)
,
addresses_array AS (
	SELECT
		application_id,
		collect_list(bureau_address_street_struct) AS addresses_json
	FROM addresses_struct
	GROUP BY 1
)
,
cellPhones_struct AS (
	SELECT
	application_id,
	NAMED_STRUCT('cellPhones', bureau_cellphone_number) AS bureau_cellphone_number_struct
	FROM {{ ref('f_kyc_bureau_contact_info_cellphones_br_logs') }}
	ORDER BY application_id, bureau_cellphone_order
)
,
cellPhones_array AS (
	SELECT
		application_id,
		collect_list(bureau_cellphone_number_struct) AS cellphones_json
	FROM cellPhones_struct
	GROUP BY 1
)
,
joined_arrays AS (
	SELECT DISTINCT
			COALESCE(aa.application_id, ca.application_id) AS application_id,
			aa.addresses_json,
			ca.cellphones_json,
			(aa.addresses_json, ca.cellphones_json ) AS communications_json,
			apps.application_cellphone
	FROM {{ ref('f_pii_applications_br') }} apps
	LEFT JOIN addresses_array aa ON aa.application_id = apps.application_id
	LEFT JOIN cellphones_array ca ON ca.application_id = apps.application_id
)
,
app_cellphone_in_bureau AS (
	SELECT
		*,
		FILTER(cellphones_json, x -> CONTAINS(x.cellPhones, application_cellphone)) AS app_cellphone_in_bureau,
		CAST(addresses_json AS STRING) AS addresses_string,
		CAST(cellphones_json AS STRING) AS cellphones_string
	FROM joined_arrays
)
SELECT
	*
FROM app_cellphone_in_bureau