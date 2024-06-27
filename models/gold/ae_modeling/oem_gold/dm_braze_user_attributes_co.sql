{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
WITH
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
    FROM {{ ref('f_applications_co') }}
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
d_identity_management_identifications_co AS (
    SELECT *
    FROM {{ ref('d_identity_management_identifications_co') }}
)
,
f_kyc_bureau_personal_info_co AS (
    SELECT *
    FROM {{ ref('f_kyc_bureau_personal_info_co') }}
)
,
ds_braze_baseline_features AS (
    SELECT *
    FROM hive_metastore.ds.braze_baseline_features --Hard-coded due to incapacity of our dbt version to reference multiple catalogs (when retrieving metadata NOT with the model references)
)
,
addi_clients_baseline_co AS (
    SELECT
        COALESCE(o.client_id,fa.client_id) AS client_id
    FROM      dm_applications AS a
    LEFT JOIN dm_originations AS o ON a.application_id = o.application_id
    LEFT JOIN (SELECT DISTINCT client_id FROM f_applications_co WHERE custom_is_privacy_policy_accepted = TRUE) AS fa ON a.client_id = fa.client_id
    LEFT JOIN (SELECT DISTINCT client_id FROM d_pii_cases_co ) AS pii ON pii.client_id = a.client_id
    WHERE a.country_code = 'CO'
          AND pii.client_id IS NULL
          AND COALESCE(o.client_id,fa.client_id) IS NOT NULL
    GROUP BY 1
)
,
d_communications_clients_co_cleaned AS (
    SELECT
        client_id,
        NULLIF(NULLIF(email,''),'REDACTED@REDACTED.COM') AS email,
        NULLIF(NULLIF(phone_number,''),'0000000000') AS phone,
        NULLIF(NULLIF(`data`:clientFullName.value,''),'REMOVED_NAME') AS full_name,
        TRIM(REGEXP_REPLACE(NULLIF(NULLIF(`data`:clientFullName.value,''),'REMOVED_NAME'), '\\s+', ' ')) AS full_name_cleaned,
        preferences:marketingPreference.status AS marketing_preference_status
    FROM d_communications_clients_co
)
,
d_identity_management_identifications_co_cleaned AS (
    SELECT
        client_id_or_user_id AS client_id, --Renamed for simplicity, as we won't be bringing store user_ids in the baseline datasoruce
        NULLIF(NULLIF(email_address,''),'REMOVED_EMAIL@REMOVED.REM') AS email,
        NULLIF(NULLIF(phone_number,''),'0000000000') AS phone
    FROM d_identity_management_identifications_co
)
,
f_kyc_bureau_personal_info_co_by_client_cleaned AS (
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
SELECT
    -- Full context: Braze Customer Engagement Platform - Data Scope: https://www.notion.so/addico/Braze-Customer-Engagement-Platform-Data-Scope-7502aa4eaa954b47a6e84f81b026e23d?pvs=4
    b.client_id,
    NULLIF(TRIM(REGEXP_REPLACE(CONCAT(COALESCE(bpi.first_name_part_one,''),' ', COALESCE(bpi.first_name_part_two,'')), '\\s+', ' ')),'') AS first_name,
    NULLIF(TRIM(REGEXP_REPLACE(CONCAT(COALESCE(bpi.last_name_part_one ,''),' ', COALESCE(bpi.last_name_part_two,'')), '\\s+', ' ')),'') AS last_name,
    imi.email,
    imi.phone,
    --Context  I: https://www.notion.so/addico/Exclusion-list-config-into-CEP-d7944d1bc25b40e09acf4c61e78c54e6?pvs=4#ce167de286bb458285e5b68df1d77430
    --Context II: https://addico.slack.com/archives/C073FFGKKHC/p1718801678897529?thread_ts=1718746810.501759&cid=C073FFGKKHC
    cc.marketing_preference_status,
    CASE
        WHEN cc.marketing_preference_status = 'IN' THEN 'opted_in'
        WHEN cc.marketing_preference_status = 'OUT' THEN 'unsubscribed'
    END AS email_subscribe,
    CASE
        WHEN cc.marketing_preference_status = 'IN' THEN 'opted_in'
        WHEN cc.marketing_preference_status = 'OUT' THEN 'subscribed'
    END AS push_subscribe,
    CASE
        WHEN cc.marketing_preference_status = 'IN' THEN 'subscribed'
        WHEN cc.marketing_preference_status = 'OUT' THEN 'unsubscribed'
    END AS subscription_group_id__marketing_sms__subscription_state,
    CASE
        WHEN cc.marketing_preference_status = 'IN' THEN 'subscribed'
        WHEN cc.marketing_preference_status = 'OUT' THEN 'unsubscribed'
    END AS subscription_group_id__marketing_wa__subscription_state,
    dsb.test_name
FROM      addi_clients_baseline_co                         AS b
LEFT JOIN f_kyc_bureau_personal_info_co_by_client_cleaned  AS bpi ON b.client_id = bpi.client_id
LEFT JOIN d_identity_management_identifications_co_cleaned AS imi ON b.client_id = imi.client_id
LEFT JOIN d_communications_clients_co_cleaned              AS cc  ON b.client_id = cc.client_id
LEFT JOIN ds_braze_baseline_features                       AS dsb ON b.client_id = dsb.client_id