{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
WITH
--/// - - - - - - - - - - - -///
--///***   A. SOURCES     ***///
--/// - - - - - - - - - - - -///
dm_applications AS (
    SELECT *
    FROM {{ ref('dm_applications') }}
)
,
dm_originations AS (
    SELECT *
    FROM {{ ref('dm_originations') }}
)
,
f_applications_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_co') }}
)
,
d_pii_cases_co AS (
    SELECT *
    FROM {{ ref('d_pii_cases_co') }}
)
,
d_communications_clients_co AS (
    SELECT *
    FROM {{ ref('d_communications_clients_co') }}
)
,
f_communications_marketing_preference_log_co AS (
    SELECT *
    FROM {{ ref('f_communications_marketing_preference_log_co') }}
)
,
d_identity_management_identifications_co AS (
    SELECT *
    FROM {{ ref('d_identity_management_identifications_co') }}
)
,
d_braze_users_behaviors_subscription_group_state  AS (
    SELECT *
    FROM {{ ref('d_braze_users_behaviors_subscription_group_state') }}
)
,
d_braze_users_behaviors_subscription_global_state AS (
    SELECT *
    FROM {{ ref('d_braze_users_behaviors_subscription_global_state') }}
)
,
aux_braze_subscription_status_ui_to_api AS (
    SELECT *
    FROM {{ ref('aux_braze_subscription_status_ui_to_api') }}
)
,
aux_braze_preference_source_applicability_to_channels_matrix AS (
    SELECT *
    FROM {{ ref('aux_braze_preference_source_applicability_to_channels_matrix') }}
)
,
f_kyc_bureau_personal_info_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_kyc_bureau_personal_info_co') }}
)
,
d_rne_client_email_preferences  AS (
    SELECT *
    FROM {{ source('silver_servicing', 'd_rne_client_email_preferences') }}
)
,
d_rne_client_phone_number_preferences AS (
    SELECT *
    FROM {{ source('silver_servicing', 'd_rne_client_phone_number_preferences') }}
)
,
ds_braze_baseline_features AS (
    SELECT *
    FROM hive_metastore.ds.braze_baseline_features --Hard-coded due to incapacity of our dbt version to reference multiple catalogs (when retrieving metadata NOT with the model references)
)
,
--/// - - - - - - - - - - - - -///
--///*** B. CLIENTS BASELINE ***///
--/// - - - - - - - - - - - - -///
addi_clients_baseline_co AS (
    -- KEY: Setting flags for all clients that has done at least one application. A flag for those who have accepted our
    --      privacy policy (addi privacy policy only), a flag for those who have originated at least once AND also one
    --      for those who have requested their PII data removed (on this matter check this link for further context on what it implies (https://www.notion.so/addico/PII-Silver-table-masking-41f73e0661d345238879cfd0f3e6a4d1?pvs=4)
    --      Then created a consolidated criteria flag based on those: `ae_complies_basic_criteria`
    SELECT
        client_id,
        has_originated,
        has_accepted_privacy_policy,
        is_in_pii_removal,
        (has_originated OR has_accepted_privacy_policy) AND NOT is_in_pii_removal AS ae_complies_basic_criteria
    FROM (
        SELECT
            COALESCE(a.client_id, o.client_id) AS client_id,
            COALESCE(MAX(o.client_id IS NOT NULL),FALSE) AS has_originated,
            COALESCE(MAX(fa.client_id IS NOT NULL),FALSE) AS has_accepted_privacy_policy,
            COALESCE(MAX(pii.client_id IS NOT NULL),FALSE) AS is_in_pii_removal
        FROM      dm_applications AS a
        LEFT JOIN dm_originations AS o ON a.application_id = o.application_id
        LEFT JOIN (SELECT DISTINCT client_id FROM f_applications_co WHERE custom_is_privacy_policy_accepted = TRUE) AS fa ON a.client_id = fa.client_id
        LEFT JOIN (SELECT DISTINCT client_id FROM d_pii_cases_co ) AS pii ON pii.client_id = a.client_id
        WHERE a.country_code = 'CO'
        GROUP BY 1
    )
)
,
--/// - - - - - - - - - - - - - - - -///
--///*** C. CLIENTS PERSONAL DATA ***///
--/// - - - - - - - - - - - - - - - -///
d_identity_management_identifications_co_cleaned AS (
    -- SECONDARY SOURCE FOR EMAIL AND PHONE as it doesn't consider updates of email and phone and has some data quality
    -- issues (already reported)
    SELECT
        client_id_or_user_id AS client_id, --Renamed for simplicity, as we won't be bringing store user_ids in the baseline datasource
        NULLIF(NULLIF(REPLACE(email_address, ' ', ''),''),'REMOVED_EMAIL@REMOVED.REM') AS email,
        NULLIF(NULLIF(phone_number,''),'0000000000') AS phone
    FROM d_identity_management_identifications_co
)
,
f_kyc_bureau_personal_info_co_by_client_cleaned AS (
    -- PRIMARY SOURCE FOR first and last name. KYC
    SELECT
        client_id,
        TRIM(REGEXP_REPLACE(NULLIF(personId_fullName,''), '\\s+', ' ')) AS full_name_cleaned,
        NULLIF(TRIM(personId_firstName),'') AS first_name_part_one,
        NULLIF(TRIM(personId_middleName),'') AS first_name_part_two,
        NULLIF(TRIM(personId_lastName),'') AS last_name_part_one,
        NULLIF(TRIM(personId_secondLastName),'') AS last_name_part_two
    FROM f_kyc_bureau_personal_info_co
    -- Filter: Only last response from bureau for each client
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY last_event_ocurred_on_processed DESC) = 1
)
,
--/// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -///
--///*** D. CLIENTS MARKETING PREFERENCE DATA (AND PERSONAL DATA FROM COMMUNICATIONS DOMAIN) ***///
--/// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -///
d_communications_clients_co_cleaned AS (
    -- PRIMARY SOURCE FOR EMAIL AND PHONE as it seems to update the email and phone according to the
    -- events triggered for that purpose. We bring full_name but this is not used for now
    -- ONE of the SOURCES for marketing communications preferences. Will depend as well on the timestamp of the preferences
    -- when compared against the secondary source
    SELECT
        client_id,
        NULLIF(NULLIF(REPLACE(email, ' ', ''),''),'REDACTED@REDACTED.COM') AS email,
        NULLIF(NULLIF(phone_number,''),'0000000000') AS phone,
        NULLIF(NULLIF(`data`:clientFullName.value,''),'REMOVED_NAME') AS full_name,
        TRIM(REGEXP_REPLACE(NULLIF(NULLIF(`data`:clientFullName.value,''),'REMOVED_NAME'), '\\s+', ' ')) AS full_name_cleaned,
        preferences:marketingPreference.status AS marketing_preference_type,
        created_at AS marketing_preference_created_at_proxy -- ONLY for cases that won't be found in the marketing preference logs
    FROM d_communications_clients_co
)
,
f_communications_marketing_preference_log_co_cleaned AS (
    -- ONE of the SOURCES for marketing communications preferences. Will depend as well on the timestamp of the preferences
    -- when compared against the secondary source
    -- Filtering only the latest preference and its timestamp
    SELECT
        client_id,
        preference_type AS marketing_preference_type,
        preference_source AS marketing_preference_source,
        preference_created_at AS marketing_preference_created_at
    FROM f_communications_marketing_preference_log_co
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY preference_created_at DESC) = 1
)
,
d_communications_marketing_preference_cleaned_unified AS (
    -- UNIFYING THE TWO TABLES ABOVE
    SELECT
        COALESCE(d.client_id, f.client_id) AS client_id,
        (COALESCE(f.marketing_preference_type, d.marketing_preference_type) = 'IN') AS marketing_preference_type_boolean, -- NULL-> NULL; IN->TRUE; OUT-> FALSE
        COALESCE(f.marketing_preference_created_at, d.marketing_preference_created_at_proxy) AS marketing_preference_latest_update,
        COALESCE(CONCAT('COMMUNICATIONS_',f.marketing_preference_source), 'COMMUNICATIONS__UNKNOWN__') AS marketing_preference_source
    FROM            d_communications_clients_co_cleaned                  AS d
    FULL OUTER JOIN f_communications_marketing_preference_log_co_cleaned AS f ON d.client_id = f.client_id
)
,
client_preferences_last_per_source_log AS (
    -- Unifying the logs of all sources (last preference status per source) in a common format
    -- Step 4: Getting values from individual custom struct
    -- Context I   : https://www.notion.so/addico/Exclusion-list-config-into-CEP-d7944d1bc25b40e09acf4c61e78c54e6?pvs=4#ce167de286bb458285e5b68df1d77430
    -- Context II  : https://addico.slack.com/archives/C073FFGKKHC/p1718801678897529?thread_ts=1718746810.501759&cid=C073FFGKKHC
    -- Context III : https://www.notion.so/addico/Comms-preferences-list-config-into-CEP-d7944d1bc25b40e09acf4c61e78c54e6#e585064cb4ca40ad9452a0f6174a948c
    -- Context IV  : https://www.notion.so/addico/RNE-implementation-84f7ff86312646918bd090f9c195bb83?pvs=4#5592c1a42e604812ac60754a93f04562
    -- Context V  : https://addico.slack.com/archives/C073FFGKKHC/p1726162091678259?thread_ts=1726161885.512659&cid=C073FFGKKHC
    SELECT
        client_id,
        preference.preference_source,
        preference.preference_value,
        preference_timestamp,
        preference_origin
    FROM
    (   -- Step 3: Exploding custom arrays of structs
        SELECT
            client_id,
            EXPLODE(preferences_array) AS preference,
            preference_timestamp,
            preference_origin
        FROM
        ( -- Step 2: Unioning all sources with the latest preferences per case
          -- WARNING! PLEASE ALIGN THE `preference_source` to the listed values in `aux_braze_preference_source_applicability_to_channels_matrix`, if new, add it to that table as well
            ( -- Step 1A: RNE Client Email preferences custom array of structs
            SELECT
                client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','rne_preference_by_email'       ,'preference_value',preference_by_email::STRING),
                    NAMED_STRUCT('preference_source','rne_preference_by_app_by_email','preference_value',preference_by_app::STRING)
                ) AS preferences_array,
                last_requested_by_client_at AS preference_timestamp,
                'RNE_EMAIL' AS preference_origin
            FROM d_rne_client_email_preferences
            )
            UNION ALL
            ( -- Step 1B:RNE Client Phone Number preferences custom array of structs
            SELECT
                client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','rne_preference_by_sms'      ,'preference_value',preference_by_sms::STRING),
                    NAMED_STRUCT('preference_source','rne_preference_by_call'     ,'preference_value',preference_by_call::STRING),
                    NAMED_STRUCT('preference_source','rne_preference_by_app_phone','preference_value',preference_by_app::STRING)
                ) AS preferences_array,
                last_requested_by_client_at AS preference_timestamp,
                'RNE_PHONE_NUMBER' AS preference_origin
            FROM d_rne_client_phone_number_preferences
            )
            UNION ALL
            ( -- Step 1C: Platform (addi) marketing preferences (on top of cleaned dataset: logs and/or last state)
            SELECT
                client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','communications_marketing_preferences','preference_value',marketing_preference_type_boolean::STRING)
                ) AS preferences_array,
                marketing_preference_latest_update AS preference_timestamp,
                marketing_preference_source AS preference_origin
            FROM d_communications_marketing_preference_cleaned_unified
            )
            UNION ALL
            ( -- Step 1D: Braze Currents Behaviour Global Subscription Event for Email, preferences
            SELECT
                sgs.external_user_id AS client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','braze_email_global_state','preference_value',aux.subscription_status_api::STRING)
                ) AS preferences_array,
                sgs.last_event_ocurred_on_processed AS preference_timestamp,
                CONCAT('BRAZE_',REPLACE(UPPER(state_change_source),' ','_')) AS preference_origin
            FROM      d_braze_users_behaviors_subscription_global_state AS sgs
            LEFT JOIN aux_braze_subscription_status_ui_to_api           AS aux ON sgs.subscription_status = aux.subscription_status_ui
            WHERE sgs.channel = 'email'
            )
            UNION ALL
            /* -- For now it's commented as has not been implemented by braze yet. When that happens make sure to update the WHERE condition as well
            ( -- Step 1E (not existent for now): Braze Currents Behaviour Global Subscription Event for Push (notifications), preferences
            SELECT
                sgs.external_user_id AS client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','braze_push_global_state','preference_value',aux.subscription_status_api::STRING)
                ) AS preferences_array,
                sgs.last_event_ocurred_on_processed AS preference_timestamp,
                CONCAT('BRAZE_',REPLACE(UPPER(state_change_source),' ','_')) AS preference_origin
            FROM      d_braze_users_behaviors_subscription_global_state AS sgs
            LEFT JOIN aux_braze_subscription_status_ui_to_api           AS aux ON sgs.subscription_status = aux.subscription_status_ui
            WHERE sgs.channel ILIKE '%push%' -- Need to validate what's the actual value when Braze implements it
            )
            UNION ALL*/
            ( -- Step 1F: Braze Currents Behaviour Subscription Group Event for SMS, preferences
            SELECT
                sgs.external_user_id AS client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','braze_sms_group_state','preference_value',aux.subscription_status_api::STRING)
                ) AS preferences_array,
                sgs.last_event_ocurred_on_processed AS preference_timestamp,
                CONCAT('BRAZE_',REPLACE(UPPER(state_change_source),' ','_')) AS preference_origin
            FROM      d_braze_users_behaviors_subscription_group_state AS sgs
            LEFT JOIN aux_braze_subscription_status_ui_to_api          AS aux ON sgs.subscription_status = aux.subscription_status_ui
            WHERE sgs.subscription_group_id  = '44909a09-f558-44f0-8293-5d3199dd559f' -- SMS subscription group
            )
            UNION ALL
            ( -- Step 1G: Braze Currents Behaviour Subscription Group Event for WhatsApp, preferences
            SELECT
                sgs.external_user_id AS client_id,
                ARRAY(
                    NAMED_STRUCT('preference_source','braze_wa_group_state','preference_value',aux.subscription_status_api::STRING)
                ) AS preferences_array,
                sgs.last_event_ocurred_on_processed AS preference_timestamp,
                CONCAT('BRAZE_',REPLACE(UPPER(state_change_source),' ','_')) AS preference_origin
            FROM      d_braze_users_behaviors_subscription_group_state AS sgs
            LEFT JOIN aux_braze_subscription_status_ui_to_api          AS aux ON sgs.subscription_status = aux.subscription_status_ui
            WHERE sgs.subscription_group_id  = '52688e31-dbf9-4ec5-8d67-3f73771bcd2f' -- WhatsApp subscription group
            )
        )
    )
)
,
client_processed_preferences_all_channels AS (
    -- Last step processing for all four channels out of the logs
    -- Step 6: Build preferences ready for CDI (valid API values) by remapping the 'true','false' values accordingly (per channel); if they are valid we keep them as such
    SELECT
        client_id,
        CASE
            WHEN latest_email_preference IN ('unsubscribed','subscribed','opted_in') THEN latest_email_preference
            WHEN latest_email_preference IN ('true','false')                         THEN IF(latest_email_preference='true','opted_in','unsubscribed')
        END AS email_subscribe, -- opted_in, unsubscribed , NULL;; subscribed
        CASE
            WHEN latest_push_preference IN ('unsubscribed','subscribed','opted_in') THEN latest_push_preference
            WHEN latest_push_preference IN ('true','false')                         THEN IF(latest_push_preference='true','opted_in','subscribed')
        END AS push_subscribe, -- opted_in, subscribed , NULL;; unsubscribed
        CASE
            WHEN latest_sms_preference IN ('unsubscribed','subscribed','opted_in') THEN latest_sms_preference
            WHEN latest_sms_preference IN ('true','false')                         THEN IF(latest_sms_preference='true','subscribed','unsubscribed')
        END AS subscription_group_id__marketing_sms__subscription_state, -- subscribed, unsubscribed , NULL;; opted_in
        CASE
            WHEN latest_whatsapp_preference IN ('unsubscribed','subscribed','opted_in') THEN latest_whatsapp_preference
            WHEN latest_whatsapp_preference IN ('true','false')                         THEN IF(latest_whatsapp_preference='true','subscribed','unsubscribed')
        END AS subscription_group_id__marketing_wa__subscription_state, -- opted_in, subscribed , NULL;; unsubscribed
        STRUCT(latest_email_preference,latest_push_preference,latest_sms_preference,latest_whatsapp_preference) AS debug_client_last_preferences,
        debug_client_last_preference_by_communications_source_log
    FROM ( -- Step 5: Get last preferences per client out of the log for all sources applicable relative to the target channel (check FILTER)
        SELECT
            cpl.client_id,
            MAX_BY(cpl.preference_value, cpl.preference_timestamp) FILTER (WHERE acm.is_email_applicable) AS latest_email_preference,
            MAX_BY(cpl.preference_value, cpl.preference_timestamp) FILTER (WHERE acm.is_push_applicable) AS latest_push_preference,
            MAX_BY(cpl.preference_value, cpl.preference_timestamp) FILTER (WHERE acm.is_sms_applicable) AS latest_sms_preference,
            MAX_BY(cpl.preference_value, cpl.preference_timestamp) FILTER (WHERE acm.is_whatsapp_applicable) AS latest_whatsapp_preference,
            COLLECT_LIST(STRUCT(cpl.preference_source, cpl.preference_value, cpl.preference_timestamp, cpl.preference_origin)) AS debug_client_last_preference_by_communications_source_log
        FROM      client_preferences_last_per_source_log                       AS cpl
        LEFT JOIN aux_braze_preference_source_applicability_to_channels_matrix AS acm ON cpl.preference_source = acm.preference_source
        GROUP BY 1
    )
)
--/// - - - - - - - - - - -///
--///*** E. FINAL TABLE ***///
--/// - - - - - - - - - - -///
SELECT
    -- Full context: Braze Customer Engagement Platform - Data Scope: https://www.notion.so/addico/Braze-Customer-Engagement-Platform-Data-Scope-7502aa4eaa954b47a6e84f81b026e23d?pvs=4
    b.client_id,
    -- A. Snapshot hard-filter flags
    b.ae_complies_basic_criteria, ---- Complies with AE criteria, further details above  on CTE: `addi_clients_baseline_co`
    dsb.to_be_tracked_braze_snapshots AS ds_to_be_tracked_braze_snapshots, -- Complies with the established business criteria (DS+PM) - Check Context IV Thread below
    -- B. Personal data and preferences
    INITCAP(NULLIF(TRIM(REGEXP_REPLACE(CONCAT(COALESCE(bpi.first_name_part_one,''),' ', COALESCE(bpi.first_name_part_two,'')), '\\s+', ' ')),'')) AS first_name,
    INITCAP(NULLIF(TRIM(REGEXP_REPLACE(CONCAT(COALESCE(bpi.last_name_part_one ,''),' ', COALESCE(bpi.last_name_part_two,'')), '\\s+', ' ')),'')) AS last_name,
    COALESCE(dcc.email, imi.email) AS email,
    COALESCE(dcc.phone, imi.phone) AS phone,
    cpp.email_subscribe,
    cpp.push_subscribe,
    cpp.subscription_group_id__marketing_sms__subscription_state,
    cpp.subscription_group_id__marketing_wa__subscription_state,
    --Context III: https://addico.slack.com/archives/C0759U6G6VC/p1721229636574799?thread_ts=1721229393.635149&cid=C0759U6G6VC
    --Context IV: https://addico.slack.com/archives/C0759U6G6VC/p1721330446806389?thread_ts=1721311090.588409&cid=C0759U6G6VC
    -- C. Additional User Attributes (from DS)
    dsb.age_group,
    dsb.gender,
    dsb.financial_index,
    dsb.app_index,
    dsb.addi_experience_index,
    dsb.tech_savvy_index,
    dsb.remaining_addicupo_bin,
    dsb.used_cupo_bin,
    dsb.is_intro,
    dsb.is_addi_plus,
    dsb.is_prospect,
    dsb.n_total_purchases,
    dsb.top_categories,
    dsb.favorite_category,
    dsb.cupo_status,
    dsb.weeks_since_last_transaction_bin,
    dsb.income,
    dsb.date_first_purchase,
    dsb.pap_psl_amount,
    dsb.pap_psl_expiration_date,
    dsb.pap_psl_segment,
    dsb.product_first_loan,
    dsb.reb_cl,
    dsb.test_name,
    -- D. Debugging the two key flags above and communication preferences
    -- is_eligible_to_be_tracked_on_braze:Business conditions to be eligible (Context: DS+PM)
    -- is_on_braze_proxy: Proxy based on DE table that Braze reads, comes from DS baseline (Context: AE+DE)
    STRUCT(dsb.is_on_braze_proxy, dsb.is_eligible_to_be_tracked_on_braze) AS debug_ds_to_be_tracked_braze_snapshots,
    -- has_originated + has_accepted_privacy_policy + is_in_pii_removal: Check criteria on CTE: `addi_clients_baseline_co`
    STRUCT(b.has_originated, b.has_accepted_privacy_policy, b.is_in_pii_removal) AS debug_ae_complies_basic_criteria,
    cpp.debug_client_last_preferences, --For the 4 channels, the preference selection
    cpp.debug_client_last_preference_by_communications_source_log, -- All sources for all 4 channels
    -- E. DATA PLATFORM DATA
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM      addi_clients_baseline_co                         AS b
LEFT JOIN f_kyc_bureau_personal_info_co_by_client_cleaned  AS bpi ON b.client_id = bpi.client_id
LEFT JOIN d_identity_management_identifications_co_cleaned AS imi ON b.client_id = imi.client_id
LEFT JOIN client_processed_preferences_all_channels        AS cpp ON b.client_id = cpp.client_id
LEFT JOIN d_communications_clients_co_cleaned              AS dcc ON b.client_id = dcc.client_id
LEFT JOIN ds_braze_baseline_features                       AS dsb ON b.client_id = dsb.client_id