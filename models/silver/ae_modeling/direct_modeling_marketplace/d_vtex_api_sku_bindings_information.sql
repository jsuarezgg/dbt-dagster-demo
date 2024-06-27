{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_sku_bindings_information_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTESSELECT
skusellerid as sku_binding_seller_id,
updatedate as sku_binding_update_date,
requestedupdatedate as sku_binding_requested_update_date,
sellerstockkeepingunitid as seller_sku_id,
sellerid as seller_id,
stockkeepingunitid as sku_id,
isactive as is_active,
ispersisted as is_persisted,
isremoved as is_removed,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('bronze','vtex_sku_bindings_information_api') }}





