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

--raw_modeling.wompitransactioncreated_co
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
    json_tmp.transaction.reference AS payment_id,
    json_tmp.client.id AS client_id,
    json_tmp.transaction.status AS wompi_transaction_status,
    json_tmp.transaction.statusReason AS wompi_transaction_status_reason,
    json_tmp.transaction.paymentMethod.financialInstitutionCode AS wompi_transaction_financial_institution_code,
    CASE
        WHEN (json_tmp.transaction.paymentMethod.type = 'NEQUI' AND json_tmp.transaction.paymentMethod.paymentDescription = 'Pago ADDI using payment source ID') THEN 'App'
        WHEN (json_tmp.transaction.paymentMethod.type = 'NEQUI' AND json_tmp.transaction.paymentMethod.paymentDescription IS NULL) THEN 'Widget'
    ELSE CAST(NULL AS STRING)
    END AS custom_nequi_payment_source,
    cast(true AS BOOLEAN) AS custom_is_wompi
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'wompitransactioncreated_co') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
