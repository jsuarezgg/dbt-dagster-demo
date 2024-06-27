
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


--raw_modeling.clientloansstatusupdatedv2_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    to_date(ocurred_on) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    json_tmp.client.addiCupo.addiCupoLastUpdateReason as addicupo_last_update_reason,
    json_tmp.client.addiCupo.remainingBalance.value as addicupo_remaining_balance,
    json_tmp.client.addiCupo.source as addicupo_source,
    json_tmp.client.addiCupo.state as addicupo_state,
    json_tmp.client.addiCupo.totalAddiCupo.value as addicupo_total,
    json_tmp.client.clientId.value as client_id,
    json_tmp.client.status.clientDelinquencyBalance.value as delinquency_balance,
    json_tmp.mode as event_mode,
    json_tmp.version as event_version,
    json_tmp.client.status.clientFullPayment.value as full_payment,
    json_tmp.client.status.clientInstallmentPayment.value as installment_payment,
    json_tmp.client.status.clientMinPayment.value as min_payment,
    json_tmp.client.ownership as ownership,
    json_tmp.client.status.clientPayDay.value as payday,
    json_tmp.client.status.positiveBalance.value as positive_balance
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'clientloansstatusupdatedv2_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
