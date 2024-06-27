{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_orders_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
orderid AS order_id,
status,
cancellationdata AS cancellation_data,
cancelreason AS cancellation_reason,
allowcancellation AS allow_cancelation,
invoiceddate::timestamp AS invoiced_date,
shippingdata AS shipping_data,
packageattachment AS package_attachment,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze','vtex_orders_api') }}