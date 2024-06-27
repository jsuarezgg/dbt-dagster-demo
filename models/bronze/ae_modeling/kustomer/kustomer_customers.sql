{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 7427516 by 2023-09-19
--raw_modeling.kus_customers
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    customerId AS customer_id,
    name AS name,
    displayName AS display_name,
    externalId AS external_id,
    externalIds_verified AS external_ids_verified,
    externalIds_id AS external_ids_id,
    sharedExternalIds AS shared_external_ids,
    emails_type AS emails_type,
    emails_verified AS emails_verified,
    emails_email AS emails_email,
    sharedEmails AS shared_emails,
    phones_type AS phones_type,
    phones_verified AS phones_verified,
    phones_phone AS phones_phone,
    sharedPhones AS shared_phones,
    whatsapps_type AS whatsapps_type,
    whatsapps_verified AS whatsapps_verified,
    whatsapps_phone AS whatsapps_phone,
    facebookIds AS facebook_ids,
    instagramIds AS instagram_ids,
    sharedSocials AS shared_socials,
    urls AS urls,
    locations AS locations,
    activeUsers AS active_users,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    lastActivityAt AS last_activity_at,
    deleted AS deleted,
    lastConversation_id AS last_conversation_id,
    conversationCounts_done AS conversation_counts_done,
    conversationCounts_open AS conversation_counts_open,
    conversationCounts_snoozed AS conversation_counts_snoozed,
    conversationCounts_all AS conversation_counts_all,
    tags AS tags,
    sentiment_polarity AS sentiment_polarity,
    sentiment_confidence AS sentiment_confidence,
    progressiveStatus AS progressive_status,
    verified AS verified,
    rev AS rev,
    defaultLang AS default_lang,
    roleGroupVersions AS role_group_versions,
    firstName AS first_name,
    lastName AS last_name,
    orgId AS org_id,
    modifiedById AS modified_by_id,
    referenceDate AS reference_date,
    year AS reference_date_year,
    month AS reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_customers') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE to_date(updatedAt) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updatedAt AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customerId ORDER BY updatedAt DESC) = 1