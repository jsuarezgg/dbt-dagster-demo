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

--raw_modeling.clientpaymentreceivedv3_br
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
    json_tmp.clientPayment.annulmentReason as annulment_reason,
    coalesce(json_tmp.clientPayment.clientId.id, json_tmp.metadata.context.clientId) as client_id,
    json_tmp.clientPayment.amount as paid_amount,
    json_tmp.clientPayment.paymentDate as payment_date,
    json_tmp.clientPayment.id.id as payment_id,
    json_tmp.clientPayment.paymentMethod as payment_method,
    -- CUSTOM ATTRIBUTES
    FALSE as is_annulled
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'clientpaymentreceivedv3_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}