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
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('client_payments_payment_pix_br') }}
