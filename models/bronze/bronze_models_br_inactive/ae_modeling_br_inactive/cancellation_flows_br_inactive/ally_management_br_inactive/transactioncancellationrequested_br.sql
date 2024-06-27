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
--raw_modeling.transactioncancellationrequested_br
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
    COALESCE(json_tmp.allyId,json_tmp.metadata.context.allyId) AS ally_slug,
    json_tmp.metadata.context.storeId AS store_slug,
    json_tmp.clientId AS id_number,
    COALESCE(json_tmp.id, CONCAT('NULL_VALUE___EVENT_ID_',json_tmp.eventId)) AS transaction_id,
    json_tmp.loanId AS loan_id,
    json_tmp.loanSource AS loan_source,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    json_tmp.metadata.context.userId AS user_id,
    json_tmp.metadata.context.userRole AS user_role,
    json_tmp.subProduct AS transaction_subproduct,
    json_tmp.cancellation.amount AS transaction_cancellation_amount,
    TO_TIMESTAMP(COALESCE(json_tmp.cancellation.date,json_tmp.cancellationDate)) AS transaction_cancellation_date,
    COALESCE(json_tmp.cancellation.reason,json_tmp.cancellationReason) AS transaction_cancellation_reason,
    COALESCE(json_tmp.cancellation.source,json_tmp.source) AS transaction_cancellation_source,
    json_tmp.cancellation.type AS transaction_cancellation_type,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'ALLY_MANAGEMENT' AS custom_event_domain,
    'REQUESTED' AS custom_transaction_cancellation_status,
    -- CAST(ocurred_on AS TIMESTAMP) AS transactioncancellationrequested_br_at -- To store it as a standalone column, when needed

    --- OLD MAPPED FIELDS, TO DEPRECATE IN NEAR FUTURE
	CAST(json_tmp.cancellationDate AS TIMESTAMP) AS cancellation_date,
    json_tmp.cancellationReason AS cancellation_reason,
	json_tmp.source AS source,
    json_tmp.cancellation.type as cancellation_type,
    json_tmp.cancellation.amount as cancellation_amount

FROM  {{source(  'raw_modeling', 'transactioncancellationrequested_br' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
