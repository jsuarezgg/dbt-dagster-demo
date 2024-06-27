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

--raw_modeling.clientpaymentpromisegenerated_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.paymentPromise.id.value AS payment_promise_id, 
    COALESCE(json_tmp.client.id.value, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.paymentPromise.state AS state,
    json_tmp.paymentPromise.expectedAmount.value AS expected_amount,
    CAST(json_tmp.paymentPromise.startDate.value AS TIMESTAMP) AS start_date,
    CAST(json_tmp.paymentPromise.endDate.value AS TIMESTAMP) AS end_date,
    json_tmp.resolutionCall AS resolution_call,
    json_tmp.agent.id.value AS agent_info,
    json_tmp.agent.code.value As agent_code,
    json_tmp.conditions AS conditions,
    -- CUSTOM ATTRIBUTES
    'ClientPaymentPromiseGenerated' AS stage
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'clientpaymentpromisegenerated_co') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}