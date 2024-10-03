
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.sanctionlistmatchdetected_unnested_by_sanctioned_individuals_co
WITH select_explode AS (
    SELECT
        -- MANDATORY FIELDS
        json_tmp.eventType AS event_name_original,
        reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
        json_tmp.eventId AS event_id,
        CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
        dt AS ocurred_on_date,
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at,
        -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
        json_tmp.application.id AS application_id,
        json_tmp.client.id AS client_id,
        json_tmp.client.type AS client_type,
        json_tmp.originationEventType AS event_type,
        json_tmp.metadata.context.allyId AS ally_slug,
        json_tmp.application.journey.name AS journey_name,
        json_tmp.application.journey.currentStage.name AS journey_stage_name,
        EXPLODE(json_tmp.sanctionedIndividuals) AS sanctioned_individuals
        -- CUSTOM ATTRIBUTES
          -- Fill with your custom attributes
        -- CAST(ocurred_on AS TIMESTAMP) AS sanctionlistmatchdetected_co -- To store it as a standalone column, when needed
    -- DBT SOURCE REFERENCE
    from {{ source('raw_modeling', 'sanctionlistmatchdetected_co') }}
    -- DBT INCREMENTAL SENTENCE

    {% if is_incremental() %}
        WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
    {% endif %}
)
SELECT
    MD5(CONCAT('EID_',COALESCE(event_id, ''),
               '_APPID_', COALESCE(application_id, ''),
               '_SLN_', COALESCE(sanctioned_individuals.sanctionListName, ''),
               '_IDN_',COALESCE(sanctioned_individuals.idNumber, ''))) AS custom_event_application_santion_list_id_number_pairing_id,
    event_name_original,
    event_name,
    event_id,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    application_id,
    client_id,
    client_type,
    event_type,
    ally_slug,
    journey_name,
    journey_stage_name,
    sanctioned_individuals.country AS sanctioned_individuals_country,
    sanctioned_individuals.fullName AS sanctioned_individuals_full_name,
    sanctioned_individuals.idNumber AS sanctioned_individuals_id_number,
    sanctioned_individuals.idType AS sanctioned_individuals_id_type,
    sanctioned_individuals.sanctionListName AS sanctioned_individuals_list_name
FROM select_explode