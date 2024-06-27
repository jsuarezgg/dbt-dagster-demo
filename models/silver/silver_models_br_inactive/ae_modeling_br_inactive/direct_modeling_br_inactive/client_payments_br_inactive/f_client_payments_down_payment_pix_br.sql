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
    application_id,
    client_id,
    data_amount,
    data_status,
    data_paymentdate_value,
    data_identificationnumber,
    national_id_number,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('client_payments_down_payment_pix_br') }}
