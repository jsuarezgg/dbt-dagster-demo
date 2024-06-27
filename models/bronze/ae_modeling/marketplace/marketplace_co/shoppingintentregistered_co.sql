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


--raw_modeling.shoppingintentregistered_co
SELECT
    -- MANDATORY FIELDS
    event_type AS event_name_original,
    reverse(split(event_type,"\\."))[0] AS event_name,
    event_id AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.ally.slug as ally_slug,
    json_tmp.shoppingIntent.id as shopping_intent_id,
    json_tmp.shoppingintent.sourcedata.category AS category,
    json_tmp.shoppingintent.sourcedata.subcategory AS subcategory,
    json_tmp.shoppingintent.sourcedata.component AS component,
    json_tmp.shoppingintent.sourcedata.screen AS screen,
    json_tmp.client.id AS client_id,
    json_tmp.shoppingIntent.campaignId as campaign_id,
    json_tmp.shoppingIntent.channel as channel,
    json_tmp.shoppingIntent.device.deviceId AS device_id,
    CAST(json_tmp.shoppingIntent.date as timestamp) AS shopping_intent_timestamp
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'shoppingintentregistered_co') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}