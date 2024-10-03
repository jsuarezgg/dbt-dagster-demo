{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- Further docs on: https://www.braze.com/docs/user_guide/data_and_analytics/data_points/?tab=billable#data-points-1
-- Check the list of attributes using config macro `gold_braze_snapshots_definition` (aliases only)
--       or with `SELECT DISTINCT column_name FROM addi_prod.gold.snp_braze_user_attributes ORDER BY 1`
SELECT
    country_code,
    snapshots_column_name,
    is_billable_datapoint::BOOLEAN AS is_billable_datapoint,
    data_type,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM VALUES
    ( 'CO', 'email'                                                    , TRUE  , 'Profile data'     ),
    ( 'CO', 'first_name'                                               , TRUE  , 'Profile data'     ),
    ( 'CO', 'last_name'                                                , TRUE  , 'Profile data'     ),
    ( 'CO', 'phone'                                                    , TRUE  , 'Profile data'     ),
    ( 'CO', 'email_subscribe'                                          , FALSE , 'Contact settings' ),
    ( 'CO', 'push_subscribe'                                           , FALSE , 'Contact settings' ),
    ( 'CO', 'subscription_group_id__marketing_sms__subscription_state' , FALSE , 'Contact settings' ),
    ( 'CO', 'subscription_group_id__marketing_wa__subscription_state'  , FALSE , 'Contact settings' ),
    ( 'CO', 'age_group'                                                , TRUE  , 'Custom attributes'),
    ( 'CO', 'gender'                                                   , TRUE  , 'Custom attributes'),
    ( 'CO', 'financial_index'                                          , TRUE  , 'Custom attributes'),
    ( 'CO', 'app_index'                                                , TRUE  , 'Custom attributes'),
    ( 'CO', 'addi_experience_index'                                    , TRUE  , 'Custom attributes'),
    ( 'CO', 'tech_savvy_index'                                         , TRUE  , 'Custom attributes'),
    ( 'CO', 'remaining_addicupo_bin'                                   , TRUE  , 'Custom attributes'),
    ( 'CO', 'used_cupo_bin'                                            , TRUE  , 'Custom attributes'),
    ( 'CO', 'is_intro'                                                 , TRUE  , 'Custom attributes'),
    ( 'CO', 'is_addi_plus'                                             , TRUE  , 'Custom attributes'),
    ( 'CO', 'is_prospect'                                              , TRUE  , 'Custom attributes'),
    ( 'CO', 'n_total_purchases'                                        , TRUE  , 'Custom attributes'),
    ( 'CO', 'top_categories'                                           , TRUE  , 'Custom attributes'),
    ( 'CO', 'favorite_category'                                        , TRUE  , 'Custom attributes'),
    ( 'CO', 'cupo_status'                                              , TRUE  , 'Custom attributes'),
    ( 'CO', 'weeks_since_last_transaction_bin'                         , TRUE  , 'Custom attributes'),
    ( 'CO', 'income'                                                   , TRUE  , 'Custom attributes'),
    ( 'CO', 'date_first_purchase'                                      , TRUE  , 'Custom attributes'),
    ( 'CO', 'pap_psl_amount'                                           , TRUE  , 'Custom attributes'),
    ( 'CO', 'pap_psl_expiration_date'                                  , TRUE  , 'Custom attributes'),
    ( 'CO', 'pap_psl_segment'                                          , TRUE  , 'Custom attributes'),
    ( 'CO', 'product_first_loan'                                       , TRUE  , 'Custom attributes'),
    ( 'CO', 'reb_cl'                                                   , TRUE  , 'Custom attributes'),
    ( 'CO', 'test_name'                                                , TRUE  , 'Custom attributes'),
    ( 'CO', 'complies_to_be_removed_criteria'                          , FALSE , 'User deletion'    ),
    ( 'CO', 'is_in_pii_removal'                                        , FALSE , 'User deletion'    ),
    ( 'CO', 'has_cupo_status_for_deletion'                             , FALSE , 'User deletion'    )
    AS tab(country_code, snapshots_column_name, is_billable_datapoint, data_type)