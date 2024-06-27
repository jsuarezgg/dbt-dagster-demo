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
--raw_modeling.pixpaymentrefunded_br
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
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.metadata.context.clientId AS client_id,
    json_tmp.metadata.context.storeId AS store_slug,
    json_tmp.metadata.context.userId AS user_id,
    json_tmp.pixPayment.amount AS pix_payment_amount,
    json_tmp.pixPayment.number AS pix_payment_number,
    TO_TIMESTAMP(json_tmp.refund.occurredOn) AS pix_payment_refund_occurred_on,
    json_tmp.refund.reason AS pix_payment_refund_reason,
    json_tmp.metadata.context.flowName AS metadata_context_flowName,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'CLIENT_PAYMENTS' AS custom_event_domain,
    'PIX_REFUNDED' AS custom_payment_refund_status
    -- CAST(ocurred_on AS TIMESTAMP) AS pixpaymentrefunded_br_at -- To store it as a standalone column, when needed
FROM  {{source(  'raw_modeling', 'pixpaymentrefunded_br' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
