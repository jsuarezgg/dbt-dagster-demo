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
--raw_modeling.transactionrequestedtocancelbyid_co
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
    COALESCE(json_tmp.ally.slug,json_tmp.metadata.context.allyId) AS ally_slug,
    json_tmp.marketplaceSubOrderChannel::STRING AS marketplace_channel,
    json_tmp.marketplaceSubOrderAlly::STRING AS marketplace_suborder_ally,
    COALESCE(json_tmp.id, CONCAT('UNKOWN_FROM_EVENT_ID_', json_tmp.eventId)) AS transaction_id,
    MD5(CONCAT(COALESCE(json_tmp.id, CONCAT('UNKOWN_FROM_EVENT_ID_', json_tmp.eventId)),
               COALESCE(json_tmp.marketplaceSubOrderAlly,''))) AS custom_transaction_marketplace_suborder_ally_pairing_id,
    MD5(CONCAT(COALESCE(json_tmp.id, CONCAT('UNKOWN_FROM_EVENT_ID_', json_tmp.eventId)),
               COALESCE(json_tmp.marketplaceSubOrderChannel,''))) AS custom_transaction_marketplace_channel_pairing_id,
    json_tmp.cancellation.amount AS transaction_cancellation_amount,
    TO_TIMESTAMP(json_tmp.cancellation.date) AS transaction_cancellation_date,
    (COALESCE(json_tmp.cancellation.reason,json_tmp.cancellationReason.value))::STRING AS transaction_cancellation_reason,
    json_tmp.channel AS transaction_cancellation_channel,
    COALESCE(json_tmp.cancellation.source,json_tmp.source) AS transaction_cancellation_source,
    json_tmp.cancellation.type AS transaction_cancellation_type,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'ALLY_MANAGEMENT' AS custom_event_domain,
    'REQUESTED_BY_ID' AS custom_transaction_cancellation_status
    -- CAST(ocurred_on AS TIMESTAMP) AS transactionrequestedtocancelbyid_co_at -- To store it as a standalone column, when needed

FROM  {{source('raw_modeling', 'transactionrequestedtocancelbyid_co' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
