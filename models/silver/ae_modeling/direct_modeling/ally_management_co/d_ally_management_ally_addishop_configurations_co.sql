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
addishop_fee,
addishop_show,
addishop_active,
grouped_allies,
hours_for_attribution,
ally_shop_slug_associated,
rewards_bnpn_discount,
rewards_bnpl_discount,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_addishop_configurations_co') }}