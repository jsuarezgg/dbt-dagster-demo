


--raw.client_payments_down_payment_pix_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    pix_number,
    application_id,
    client_id,
    get_json_object(data, "$.amount") as data_amount,
    get_json_object(data, "$.status") as data_status,
    CAST(get_json_object(data, "$.paymentDate.value") AS TIMESTAMP) as data_paymentdate_value,
    get_json_object(data, "$.identificationNumber") as data_identificationnumber,
    national_id_number,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.client_payments_down_payment_pix_br