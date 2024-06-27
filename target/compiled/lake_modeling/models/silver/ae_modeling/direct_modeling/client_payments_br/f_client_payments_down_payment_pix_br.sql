



SELECT
    pix_number,
    application_id,
    client_id,
    data_amount,
    data_status,
    data_paymentdate_value,
    data_identificationnumber,
    national_id_number,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.client_payments_down_payment_pix_br