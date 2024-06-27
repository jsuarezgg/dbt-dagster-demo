


SELECT
    communication_id,
    communication_template,
    communication_version,
    communication_status,
    communication_source,
    communication_key,
    created_at,
    campaign_name,
    channel,
    recipient,
    message,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM bronze.communications_communication_br