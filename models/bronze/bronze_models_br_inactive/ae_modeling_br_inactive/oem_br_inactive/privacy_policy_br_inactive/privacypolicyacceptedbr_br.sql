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


--raw_modeling.privacypolicyacceptedbr_br
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
    json_tmp.client.birth.date AS birth_date,
    json_tmp.client.name.firstLast AS first_last_name,
    json_tmp.client.name.first AS first_name,
    json_tmp.client.name.full AS full_name,
    json_tmp.client.nationalIdentification.number AS id_number,
    json_tmp.client.nationalIdentification.type AS id_type,
    json_tmp.client.id AS client_id,
    json_tmp.application.id AS application_id,
    json_tmp.ally.slug AS ally_slug,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.originationEventType AS event_type,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.client.type AS client_type,
    COALESCE(json_tmp.application.channel,json_tmp.metadata.context.channel) AS channel,
    json_tmp.client.cellPhone.number AS application_cellphone,
    json_tmp.client.email.address AS application_email,
    json_tmp.application.marketingCampaign.id AS campaign_id,
    json_tmp.application.product AS product,
    -- CUSTOM ATTRIBUTES
    CAST(TRUE AS BOOLEAN) AS custom_is_privacy_policy_accepted
    -- CAST(ocurred_on AS TIMESTAMP) AS privacypolicyacceptedbr_br_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'privacypolicyacceptedbr_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}