{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- Values brought at the time of deployment from these links (requires access and login via Okta):
-- Braze Production Workspace: https://dashboard-07.braze.com/app_settings/developer_console/apisettings/6630053757299e00575f1f5c?locale=en#appidentifiers
-- Braze Development Workspace: https://dashboard-07.braze.com/app_settings/developer_console/apisettings/663005402b78ed005829bc4c?locale=en#appidentifiers
SELECT
    subscription_group_id,
    channel,
    custom_subscription_group_name,
    added_to_auxiliary_at_date::DATE AS added_to_auxiliary_at_date,
    description,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- NOTE TO ANALYTICS ENGINEERS: When adding new records please make sure to set `added_to_auxiliary_at_date` as the current day you're adding them
FROM VALUES
    ('44909a09-f558-44f0-8293-5d3199dd559f','SMS/MMS' ,'marketing_sms'      , '2024-09-24', NULL ),
    ('52688e31-dbf9-4ec5-8d67-3f73771bcd2f','WhatsApp','marketing_wa'       , '2024-09-24', 'Users must be subscribed to this group to receive WhatsApp messages from the marketing Addi - Braze WhatsApp number'),
    ('c120561a-2d69-450d-a29f-228b1365c104','Email'   ,'marketing_email_dev', '2024-09-24', NULL ),
    ('099de486-7e89-4224-9509-532c81c50622','WhatsApp','marketing_sms_dev'  , '2024-09-24', NULL),
    ('99774e23-eacf-4c6c-91d5-620ff36efad5','SMS/MMS' ,'marketing_wa_dev'   , '2024-09-24', 'Users must be subscribed to this group to receive WhatsApp messages from the marketing Addi - Braze WhatsApp number' )
    AS tab(subscription_group_id, channel, custom_subscription_group_name, added_to_auxiliary_at_date,description)