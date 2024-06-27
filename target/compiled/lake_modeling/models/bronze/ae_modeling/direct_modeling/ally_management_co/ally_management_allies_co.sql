


--raw.ally_management_allies_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    slug AS ally_slug,
    name AS ally_name,
    vertical,
    brand,
    website,
    type,
    tags,
    categories,
    active AS active_ally,
    logo_url,
    economic_activity,
    logos,
    channel,
    ally_state,
    commercial_type,
    similars,
    acceptance_terms_conditions,
    additional_information,
    categories_v2,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.ally_management_allies_co