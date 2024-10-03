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
	FROM {{ source('silver_live', 'f_kyc_bureau_contact_info_addresses_co_logs') }}
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
mails_struct AS (
	SELECT
	application_id,
	NAMED_STRUCT('mails', bureau_mail_address) AS bureau_mail_address_struct
	FROM {{ source('silver_live', 'f_kyc_bureau_contact_info_mails_co_logs') }}
	ORDER BY application_id, bureau_mail_order
)
,
mails_array AS (
	SELECT
		application_id,
		collect_list(bureau_mail_address_struct) AS emails_json
	FROM mails_struct
	GROUP BY 1
)
,
cellPhones_struct AS (
	SELECT
	application_id,
	NAMED_STRUCT('cellPhones', bureau_cellphone_number, "firstReport", bureau_cellphone_firstReport, "lastReport", bureau_cellphone_lastReport) AS bureau_cellphone_number_struct
	FROM {{ source('silver_live', 'f_kyc_bureau_contact_info_cellphones_co_logs') }}
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
			COALESCE(aa.application_id, ma.application_id, ca.application_id) AS application_id,
			aa.addresses_json,
			ca.cellphones_json,
			ma.emails_json,
			(aa.addresses_json, ma.emails_json, ca.cellphones_json ) AS communications_json
	FROM {{ source('silver_live', 'f_applications_co') }} apps
	LEFT JOIN addresses_array aa ON aa.application_id = apps.application_id
	LEFT JOIN mails_array ma ON ma.application_id = apps.application_id
	LEFT JOIN cellphones_array ca ON ca.application_id = apps.application_id
)
,
application_data AS (
	SELECT
		ja.*,
		app.application_cellphone,
        kycpi.personId_expeditionCity as id_exp_city
	FROM joined_arrays ja
	LEFT JOIN {{ source('silver_live', 'f_pii_applications_co') }} app 	        ON ja.application_id = app.application_id
    LEFT JOIN {{ source('silver_live', 'f_kyc_bureau_personal_info_co') }} kycpi 	ON ja.application_id = kycpi.application_id
)
,
app_cellphone_in_bureau AS (
	SELECT
		*,
		FILTER(cellphones_json, x -> CONTAINS(x.cellPhones, application_cellphone)) AS app_cellphone_in_bureau,
		CAST(addresses_json AS STRING) AS addresses_string,
		CAST(cellphones_json AS STRING) AS cellphones_string,
		CAST(emails_json AS STRING) AS emails_string
	FROM application_data
)
SELECT
	*,
	SORT_ARRAY(app_cellphone_in_bureau.firstReport)[0] as app_cellphone_in_bureau_first_report,
	SORT_ARRAY(app_cellphone_in_bureau.lastReport, FALSE)[0] as app_cellphone_in_bureau_last_report,
	SORT_ARRAY(cellphones_json.firstReport)[0] as cellphones_first_report,
	SORT_ARRAY(cellphones_json.firstReport, FALSE)[0] as cellphones_last_report
FROM app_cellphone_in_bureau