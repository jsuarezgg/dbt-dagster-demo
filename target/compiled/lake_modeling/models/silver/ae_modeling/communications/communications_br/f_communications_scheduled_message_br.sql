


SELECT
    id,
    template,
    purpose,
    source,
    fields,
    scheduled_on,
    client,
    tags,
    campaign,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.communications_scheduled_message_br