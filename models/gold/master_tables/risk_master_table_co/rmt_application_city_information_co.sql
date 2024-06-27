{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
	bpi.application_id,
    bpi.personId_expeditionCity AS id_expedition,
	aci.shipping_address,
	aci.shipping_city
FROM {{ ref('f_kyc_bureau_personal_info_co') }} bpi
LEFT JOIN {{ ref('f_applications_order_details_co') }} aci      ON bpi.application_id = aci.application_id
