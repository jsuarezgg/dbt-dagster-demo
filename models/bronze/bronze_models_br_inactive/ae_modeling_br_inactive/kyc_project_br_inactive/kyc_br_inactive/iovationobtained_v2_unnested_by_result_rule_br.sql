{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--raw_modeling.iovationobtained_v2_br
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
        -- MAPPED FIELDS
        json_tmp.metadata.context.traceId AS application_id,
        COALESCE(json_tmp.client.id,json_tmp.iovation.clientId) AS client_id,
        EXPLODE(json_tmp.iovation.data.details.ruleResults.rules) AS item,
        -- CUSTOM ATTRIBUTES
        'V2' AS custom_kyc_event_version
        -- CAST(ocurred_on AS TIMESTAMP) AS iovationobtained_v2_br_at -- To store it as a standalone column, when needed
    FROM  {{source(  'raw_modeling', 'iovationobtained_v2_br' )}}
    -- DBT INCREMENTAL SENTENCE

    {% if is_incremental() %}
        WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
    {% endif %}

)
SELECT
    -- ITEM FIELDS
    CONCAT('EID_',event_id,'_IPI_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A')) AS surrogate_key,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A') AS item_pseudo_idx,
    -- MANDATORY FIELDS
    event_name_original,
    event_name,
    event_id,
    application_id,
    client_id,
    'json_tmp.iovation.data.details.ruleResults.rules._ARRAY_' AS array_parent_path,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    -- SPECIAL DOUBLE ARRAYS - ITEM ARRAYS
    -- ITEM INNER FIELDS
    item.reason AS iovation_rule_result_reason,
    item.score AS iovation_rule_result_score,
    item.type AS iovation_rule_result_type,
    -- CUSTOM ATTRIBUTES
    custom_kyc_event_version

FROM select_explode