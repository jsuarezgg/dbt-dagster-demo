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

--raw_modeling.clientloanapplicationcreated_br
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
    coalesce(
              json_tmp.clientLoanApplication.applicationId.id,
              json_tmp.clientLoanApplication.applicationId.value,
              json_tmp.metadata.context.traceId
    ) AS application_id,
    coalesce(
              json_tmp.clientLoanApplication.allyId.slug,
              json_tmp.metadata.context.allyId
    ) AS ally_slug,
    json_tmp.clientLoanApplication.client.cellPhoneNumber AS application_cellphone,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS application_date,
    json_tmp.clientLoanApplication.applicationType AS application_channel_legacy,
    json_tmp.clientLoanApplication.client.email AS application_email,
    coalesce(
              json_tmp.clientLoanApplication.client.id.id,
              json_tmp.metadata.context.clientId
    ) AS client_id,
    json_tmp.clientLoanApplication.client.idNumber AS id_number,
    json_tmp.clientLoanApplication.client.idType AS id_type,
    json_tmp.clientLoanApplication.requestedAmount AS requested_amount,
    coalesce(
              json_tmp.clientLoanApplication.storeId.slug,
              json_tmp.metadata.context.storeId
    ) AS store_slug,
    coalesce(
              json_tmp.clientLoanApplication.storeUserId,
              json_tmp.metadata.context.userId,
              json_tmp.metadata.context.storeUserId
    ) AS store_user_id,
    -- CUSTOM ATTRIBUTES,
    cast(True AS BOOLEAN) AS custom_is_returning_client_legacy,
    'LEGACY' as custom_platform_version
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'clientloanapplicationcreated_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}