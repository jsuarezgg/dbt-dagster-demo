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


--raw_modeling.transactionattributable_br
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
    json_tmp.transaction.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.shoppingIntentAssociated.id as shopping_intent_id,
    json_tmp.metadata.context.deviceId AS device_id
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'transactionattributable_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}