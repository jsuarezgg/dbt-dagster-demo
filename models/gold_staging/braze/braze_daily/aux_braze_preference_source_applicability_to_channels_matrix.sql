{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- SECOND AND MOST IMPORTANT AUXILIARY TABLE FOR MARKETING PREFERENCES: Used on `dm_target_for_snp_braze_user_attributes_co`
-- WARNING! PLEASE ALIGN THE `preference_source` to the values in the DE CTE called: `client_preferences_last_per_source_log`
SELECT
    preference_source,
    is_email_applicable::BOOLEAN AS is_email_applicable,
    is_push_applicable::BOOLEAN AS is_push_applicable,
    is_sms_applicable::BOOLEAN AS is_sms_applicable,
    is_whatsapp_applicable::BOOLEAN AS is_whatsapp_applicable,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM VALUES
    ('rne_preference_by_sms'               ,FALSE,FALSE,TRUE ,FALSE),
    ('rne_preference_by_call'              ,FALSE,FALSE,FALSE,FALSE),
    ('rne_preference_by_app_phone_number'  ,FALSE,TRUE ,FALSE,FALSE),
    ('rne_preference_by_email'             ,TRUE ,FALSE,FALSE,FALSE),
    ('rne_preference_by_app_by_email'      ,FALSE,TRUE ,FALSE,FALSE),
    ('communications_marketing_preferences',TRUE ,TRUE ,TRUE ,TRUE ),
    ('braze_email_global_state'            ,TRUE ,FALSE,FALSE,FALSE),
    ('braze_push_global_state'             ,FALSE,TRUE ,FALSE,FALSE),
    ('braze_sms_group_state'               ,FALSE,FALSE,TRUE ,FALSE),
    ('braze_wa_group_state'                ,FALSE,FALSE,FALSE,TRUE )
AS tab(preference_source, is_email_applicable, is_push_applicable, is_sms_applicable, is_whatsapp_applicable)