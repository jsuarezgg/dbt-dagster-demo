


--raw.client_payments_payment_pix_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    pix_number,
    get_json_object(data,  "$.applicationId") as application_id,
    coalesce(client_id, get_json_object(data, "$.clientId.value")) as client_id,
    coalesce(get_json_object(data, "$.id"), id) as id,
    CAST(get_json_object(data, "$.paidDate") AS TIMESTAMP) as data_paiddate,
    coalesce(get_json_object(data, "$.totalAmount"),
             get_json_object(data, "$.pixInformation.amount")) as data_totalamount,
    get_json_object(data, "$.totalAmountPaid") as data_totalamountpaid,
    get_json_object(data, "$.paymentAccount") as data_paymentaccount,
    get_json_object(data, "$.paymentPending") as data_paymentpending,
    CAST(get_json_object(data, "$.pixInformation.dueDate") AS TIMESTAMP) as data_duedate,
    CAST(get_json_object(data, "$.pixInformation.generationDate") AS TIMESTAMP) as data_generationdate,
    substring(get_json_object(data, "$.pixInformation.paymentLimitDate"), 1, 19) as data_paymentlimitday,
    get_json_object(data, "$.paymentPixStatus") as data_paymentpixstatus,
    CAST(get_json_object(data, "$.statusUpdateDate") AS TIMESTAMP) as data_statusupdatedate,
    get_json_object(data, "$.originationChannel.value") as data_originationchannel,
    get_json_object(data, "$.pixInformation.brCode") as data_brcode,
    get_json_object(data, "$.pixInformation.pdfUrl") as data_pdfurl,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.client_payments_payment_pix_br