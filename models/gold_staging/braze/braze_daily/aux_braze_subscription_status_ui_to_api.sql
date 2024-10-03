{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- FIRST AUXILIARY TABLE FOR MARKETING PREFERENCES: Used on `dm_target_for_snp_braze_user_attributes_co`
SELECT
    subscription_status_ui,
    subscription_status_api,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM VALUES
    ('Unsubscribed', 'unsubscribed'),
    ('Subscribed'  , 'subscribed'),
    ('Opted In'    , 'opted_in')
AS tab(subscription_status_ui, subscription_status_api)