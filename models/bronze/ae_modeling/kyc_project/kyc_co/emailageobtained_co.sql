{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--raw_modeling.emailageobtained_co
SELECT 
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.applicationId AS application_id,
    json_tmp.prospectId AS client_id,
    json_tmp.emailAge.advice.id AS emailAge_advice_id,
    json_tmp.emailAge.advice.value AS emailAge_advice_value,
    json_tmp.emailAge.birthDate AS emailAge_birthDate,
    json_tmp.emailAge.company AS emailAge_company,
    json_tmp.emailAge.country AS emailAge_country,
    TO_TIMESTAMP(json_tmp.emailAge.domain.age) AS emailAge_domain_age,
    json_tmp.emailAge.domain.creationDays AS emailAge_domain_creationDays,
    json_tmp.emailAge.domain.exits AS emailAge_domain_exits,
    json_tmp.emailAge.domain.name AS emailAge_domain_name,
    json_tmp.emailAge.domain.riskLevel AS emailAge_domain_riskLevel,
    json_tmp.emailAge.domain.riskLevelId AS emailAge_domain_riskLevelId,
    json_tmp.emailAge.eName AS emailAge_eName,
    TO_TIMESTAMP(json_tmp.emailAge.email.age) AS emailAge_email_age,
    json_tmp.emailAge.email.creationDays AS emailAge_email_creationDays,
    json_tmp.emailAge.email.exists AS emailAge_email_exists,
    json_tmp.emailAge.email.value AS emailAge_email_value,
    json_tmp.emailAge.firstSeenDays AS emailAge_firstSeenDays,
    TO_TIMESTAMP(json_tmp.emailAge.firstVerificationDate) AS emailAge_firstVerificationDate,
    json_tmp.emailAge.fraudType AS emailAge_fraudType,
    json_tmp.emailAge.gender AS emailAge_gender,
    json_tmp.emailAge.hits.total AS emailAge_hits_total,
    json_tmp.emailAge.hits.unique AS emailAge_hits_unique,
    json_tmp.emailAge.imageUrl AS emailAge_imageUrl,
    TO_TIMESTAMP(json_tmp.emailAge.lastVerificationDate) AS emailAge_lastVerificationDate,
    TO_TIMESTAMP(json_tmp.emailAge.lastflaggedon) AS emailAge_lastflaggedon,
    json_tmp.emailAge.location AS emailAge_location,
    json_tmp.emailAge.reason.id AS emailAge_reason_id,
    json_tmp.emailAge.reason.value AS emailAge_reason_value,
    json_tmp.emailAge.riskBand.id AS emailAge_riskBand_id,
    json_tmp.emailAge.riskBand.value AS emailAge_riskBand_value,
    json_tmp.emailAge.score AS emailAge_score,
    json_tmp.emailAge.socialMediaFriends AS emailAge_socialMediaFriends,
    json_tmp.emailAge.sourceIndustry AS emailAge_sourceIndustry,
    json_tmp.emailAge.status.id AS emailAge_status_id,
    json_tmp.emailAge.status.value AS emailAge_status_value,
    json_tmp.emailAge.title AS emailAge_title,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS emailageobtained_co_at -- To store it as a standalone column, when needed
FROM  {{source('raw_modeling', 'emailageobtained_co' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
