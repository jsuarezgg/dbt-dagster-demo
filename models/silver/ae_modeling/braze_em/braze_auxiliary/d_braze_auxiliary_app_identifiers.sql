{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- Values brought at the time of deployment from these links (requires access and login via Okta):
-- Braze Production Workspace: https://dashboard-07.braze.com/users/subscription_groups/subscription_groups/6630053757299e00575f1f5c
-- Braze Development Workspace: https://dashboard-07.braze.com/users/subscription_groups/subscription_groups/663005402b78ed005829bc4c
SELECT
    app_id,
    custom_app_name,
    added_to_auxiliary_at_date::DATE AS added_to_auxiliary_at_date,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- NOTE TO ANALYTICS ENGINEERS: When adding new records please make sure to set `added_to_auxiliary_at_date` as the current day you're adding them
FROM VALUES
    ('3dcb74c0-27f2-417f-9484-721547f4ed6f','Addi Shop - Production (Android)', '2024-09-24'),
    ('a1c0ffed-61b2-4959-859f-48a5c9ff8b53','Addi Shop - Production (iOS)'    , '2024-09-24'),
    ('8991556f-75c1-44e6-968a-7050026430c5','Addi Shop Dev (Android)'         , '2024-09-24'),
    ('442dcfa0-0655-4f63-bf50-320a8c5a8f67','Addi Shop Dev (iOS)'             , '2024-09-24')
    AS tab(app_id, custom_app_name, added_to_auxiliary_at_date)