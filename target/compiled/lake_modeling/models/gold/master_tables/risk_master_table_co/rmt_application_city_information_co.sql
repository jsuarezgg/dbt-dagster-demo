

SELECT
	bpi.application_id,
    bpi.personId_expeditionCity AS id_expedition,
	aci.shipping_address,
	aci.shipping_city
FROM silver.f_kyc_bureau_personal_info_co bpi
LEFT JOIN silver.f_applications_order_details_co aci      ON bpi.application_id = aci.application_id