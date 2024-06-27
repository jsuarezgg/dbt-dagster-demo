{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        partition_by=['ocurred_on_date'],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.prospectloanapplicationcreatedfromcheckpoint_br
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
              json_tmp.application.id,
              json_tmp.metadata.context.traceId
    ) AS application_id,
    coalesce(
              json_tmp.application.allyId,
              json_tmp.metadata.context.allyId
    ) AS ally_slug,
    json_tmp.application.prospect.cellPhone AS application_cellphone,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS application_date,
    json_tmp.application.prospect.email AS application_email,
    json_tmp.application.applicationType AS application_channel_legacy,
    coalesce(
              json_tmp.application.prospect.id,
              json_tmp.metadata.context.clientId
    ) AS client_id,
    json_tmp.application.prospect.idNumber AS id_number,
    json_tmp.application.prospect.idType AS id_type,
    json_tmp.application.requestedAmount AS requested_amount,
    coalesce(
              json_tmp.application.storeId,
              json_tmp.metadata.context.storeId
    ) AS store_slug,
    coalesce(
              json_tmp.metadata.context.userId,
              json_tmp.metadata.context.storeUserId
    ) AS store_user_id,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    -- CUSTOM ATTRIBUTES
    CAST(True AS BOOLEAN) AS custom_is_checkpoint_application_legacy,
    'LEGACY' as custom_platform_version
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'prospectloanapplicationcreatedfromcheckpoint_br') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}