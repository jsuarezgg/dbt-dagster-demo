

--raw.employees_sheet
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    _airbyte_ab_id,
    cast(_airbyte_emitted_at as TIMESTAMP),
    ID,
    __ID,
    to_date(START_DATE, 'M/d/yyyy') as START_DATE,
    _airbyte_additional_properties,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.employees_sheet