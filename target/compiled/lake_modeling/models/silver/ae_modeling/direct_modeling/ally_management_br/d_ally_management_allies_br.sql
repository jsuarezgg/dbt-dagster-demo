

SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    ally_slug,
    ally_name,
    vertical:['name']:['value'] vertical_name,
    vertical:['slug']:['value'] vertical_slug,
    vertical:['isNew']:['value'] vertical_isnew,
    brand:['name']:['value'] brand_name,
    brand:['slug']:['value'] brand_slug,
    website,
    type,
    categories:[0]:['value'] category,
    active_ally,
    channel,
    ally_state,
    commercial_type,
    additional_information:['address']:['lineOne'] address_lineone,
    additional_information:['address']:['postalCode'] address_postalcode,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.ally_management_allies_br