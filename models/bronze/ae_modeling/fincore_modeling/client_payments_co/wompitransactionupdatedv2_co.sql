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

--raw_modeling.wompitransactionupdated_co
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
    json_tmp.client.id AS client_id,
    json_tmp.transaction.amount AS transaction_amount,
    json_tmp.transaction.currency AS transaction_currency,
    json_tmp.transaction.origin AS transaction_origin,
    json_tmp.transaction.paymentMethod.extra.businessAgreementCode AS transaction_business_agreement_code,
    json_tmp.transaction.paymentMethod.extra.paymentIntentionIdentifier AS transaction_payment_intention_identifier,
    json_tmp.transaction.paymentMethod.financialInstitutionCode AS transaction_financial_institution_code,
    json_tmp.transaction.paymentMethod.paymentDescription AS transaction_payment_description,
    json_tmp.transaction.paymentMethod.type AS transaction_type,
    json_tmp.transaction.reference AS transaction_reference,
    json_tmp.transaction.status AS transaction_status,
    json_tmp.transaction.statusReason AS transaction_status_reason
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'wompitransactionupdated_co') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
