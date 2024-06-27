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
    allies.active_ally AS ally_active,
    allies.ally_name AS ally_name,
    allies.ally_state AS ally_state,
    allies.brand AS ally_brand,
    allies.categories AS ally_categories,
    allies.channel AS ally_channel,
    allies.commercial_type AS ally_commercial_type,
    allies.economic_activity AS ally_economic_activity,
    allies.logos AS ally_logos,
    allies.tags AS ally_tags,
    allies.type AS ally_type,
    allies.vertical AS ally_vertical,
    allies.website AS ally_website,
    stores.active_store AS store_active,
    stores.address AS store_address,
    stores.ally_slug AS ally_slug,
    stores.city_code AS store_city_code,
    stores.latitude AS store_latitude,
    stores.longitude AS store_longitude,
    stores.phone_number AS store_phone_number,
    stores.schedule AS store_schedule,
    stores.store_format_id AS store_format_id,
    stores.store_name AS store_name,
    stores.store_slug AS store_slug,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM      {{ ref('ally_management_allies_br') }} AS allies
FULL JOIN {{ ref('ally_management_stores_br') }} AS stores ON stores.ally_slug=allies.ally_slug

