
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


--raw_modeling.restrictedentitysuspected_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.restrictedEntity.clientId AS client_id,
    json_tmp.restrictedEntity.createdAt AS restricted_entity_created_at,
    json_tmp.restrictedEntity.journey AS restricted_entity_journey,
    json_tmp.restrictedEntity.reason AS restricted_entity_reason,
    json_tmp.restrictedEntity.reference AS restricted_entity_reference,
    json_tmp.restrictedEntity.source AS restricted_entity_source,
    json_tmp.restrictedEntity.status AS restricted_entity_status,
    json_tmp.restrictedEntity.type AS restricted_entity_type,
    json_tmp.restrictedEntity.value AS restricted_entity_value,
    json_tmp.restrictedEntity.additionalAttributes AS additional_attributes,
    {{ dbt_utils.surrogate_key(['json_tmp.restrictedEntity.type', 'json_tmp.restrictedEntity.value']) }} AS surrogate_key
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS restrictedentitysuspected_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'restrictedentitysuspected_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
