


SELECT
    notification_id,
    communication_id,
    notification_status,
    notification_updated_at,
    id,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.communications_notifications_status_br