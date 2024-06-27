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
--raw_modeling.loancancellationorderannulledv2_br
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
    json_tmp.annulmentReason AS loan_cancellation_annulment_reason,
    json_tmp.cancellationId.id AS loan_cancellation_id,
    json_tmp.clientId.id AS client_id,
    json_tmp.loanId.id AS loan_id,
    json_tmp.metadata.context.userId AS user_id,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    -- CUSTOM ATTRIBUTES
    'CLIENT_MANAGEMENT' AS custom_event_domain,
    'V2_CANCELLATION_ANNULLED' AS custom_loan_cancellation_status,
    -- CAST(ocurred_on AS TIMESTAMP) AS loancancellationorderannulledv2_br_at -- To store it as a standalone column, when needed

    --- OLD MAPPED FIELDS, TO DEPRECATE IN NEAR FUTURE
    json_tmp.annulmentReason AS annulment_reason,
    json_tmp.cancellationId.id as cancellation_id
FROM  {{source(  'raw_modeling', 'loancancellationorderannulledv2_br' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
