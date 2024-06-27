
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.prospectupgradedtoclient_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    coalesce(json_tmp.applicationId.id,
             json_tmp.applicationId.value,
             json_tmp.metadata.context.traceId) AS application_id,
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.clientInfo.email AS application_email,
    json_tmp.loan.approvedAmount as approved_amount,
    json_tmp.loan.effectiveAnnualRate as effective_annual_rate,
    json_tmp.loan.isLowBalanceV2.value as lbl,
    json_tmp.loan.loanId.id as loan_id,
    to_date(json_tmp.ocurredOn) AS origination_date,
    cast(false as BOOLEAN) as returning_client,
    json_tmp.metadata.context.storeId as store_slug,
    coalesce(json_tmp.metadata.context.storeUserId,
             json_tmp.metadata.context.userId) as store_user_id,
    json_tmp.loan.term as term,
    coalesce(json_tmp.clientInfo.prospectId.id,
             json_tmp.metadata.context.clientId) as client_id,
    json_tmp.clientInfo.prospectInfo.commercialInfo.scores.score as credit_score
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'prospectupgradedtoclient_br') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
