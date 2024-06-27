


--raw.ally_management_stores_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    slug AS store_slug,
    latitude,
    longitude,
    name AS store_name,
    ally_slug,
    city_code,
    store_format_id,
    address,
    phone_number,
    schedule,
    active AS active_store,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.ally_management_stores_br