


SELECT
    pix_number,
    client_id,
    application_id,
    data_paiddate,
    data_totalamount,
    data_totalamountpaid,
    data_paymentaccount,
    data_paymentpending,
    data_duedate,
    data_generationdate,
    data_paymentlimitday,
    data_paymentpixstatus,
    data_statusupdatedate,
    data_originationchannel,
    data_brcode,
    data_pdfurl,
    id,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.client_payments_payment_pix_br