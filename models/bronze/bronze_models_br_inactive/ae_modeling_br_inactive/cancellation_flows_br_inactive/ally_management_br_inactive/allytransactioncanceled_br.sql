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
--raw_modeling.allytransactioncanceled_br
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
    json_tmp.metadata.context.storeId AS store_slug,
    json_tmp.client.nationalIdentification.number AS id_number,
    json_tmp.client.nationalIdentification.type AS id_type,
    COALESCE(json_tmp.transaction.id, CONCAT('NULL_VALUE___EVENT_ID_',json_tmp.eventId)) AS transaction_id,
    json_tmp.loan.id AS loan_id,
    json_tmp.metadata.context.clientId AS client_id,
    json_tmp.metadata.context.userId AS user_id,
    json_tmp.metadata.context.userRole AS user_role,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    TO_TIMESTAMP(json_tmp.transaction.cancellation.date) AS transaction_cancellation_date,
    json_tmp.transaction.cancellation.reason AS transaction_cancellation_reason,
    json_tmp.transaction.cancellation.amount AS transaction_cancellation_amount,
    json_tmp.transaction.cancellation.id AS transaction_cancellation_id,
    json_tmp.transaction.cancellation.source AS transaction_cancellation_source,
    json_tmp.transaction.cancellation.type AS transaction_cancellation_type,
    json_tmp.transaction.subProduct AS transaction_subproduct,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'ALLY_MANAGEMENT' AS custom_event_domain,
    'COMPLETED' AS custom_transaction_cancellation_status,
    -- CAST(ocurred_on AS TIMESTAMP) AS allytransactioncanceled_br_at -- To store it as a standalone column, when needed

    --- OLD MAPPED FIELDS, TO DEPRECATE IN NEAR FUTURE
    json_tmp.transaction.cancellation.date AS cancellation_date,
    json_tmp.transaction.cancellation.reason AS cancellation_reason

FROM  {{source(  'raw_modeling', 'allytransactioncanceled_br' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
