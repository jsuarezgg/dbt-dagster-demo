
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


--raw_modeling.clientloancondonationcreated_co
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
    json_tmp.client.id.value AS client_id,
    json_tmp.loan.id.value AS loan_id,
    json_tmp.condonation.amount.value AS condonation_amount,
    json_tmp.condonation.bucket AS condonation_bucket,
    CAST(json_tmp.condonation.date.value AS TIMESTAMP) AS condonation_date,
    json_tmp.condonation.id.value AS condonation_id,
    json_tmp.condonation.percentage.value AS condonation_percentage,
    json_tmp.condonation.reason.value AS condonation_reason,
    json_tmp.condonation.type AS condonation_type
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'clientloancondonationcreated_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
