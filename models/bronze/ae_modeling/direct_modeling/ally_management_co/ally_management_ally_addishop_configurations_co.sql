{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.communications_notifications_status_br
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
id,
ally_slug,
source,
created_at,
start_date_validity,
end_date_validity,
data:addiShop:fee as addishop_fee,
data:addiShop:show as addishop_show,
data:addiShop:active as addishop_active,
data:groupedAllies AS grouped_allies,
data:addiShop:hoursForAttribution AS hours_for_attribution,
data:rewards:bnpn:discount AS rewards_bnpn_discount,
data:rewards:bnpl:discount AS rewards_bnpl_discount,
ally_shop_slug_associated,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_ally_addishop_configurations_co') }}
