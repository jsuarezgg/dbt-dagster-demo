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


--raw_modeling.prospectloanapplicationcreatedfromcheckpoint_co
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
    COALESCE(json_tmp.application.allyId, json_tmp.metadata.context.allyId) AS ally_slug,
    COALESCE(json_tmp.application.id, json_tmp.metadata.context.traceId) AS application_id,
    CAST(ocurred_on AS TIMESTAMP) AS application_date,
    json_tmp.application.prospect.cellPhone AS application_cellphone,
    json_tmp.application.applicationType AS application_channel_legacy,
    json_tmp.application.prospect.email AS application_email,
    COALESCE(json_tmp.application.prospect.id, json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.application.prospect.idNumber AS id_number,
    json_tmp.application.prospect.idType AS id_type,
    json_tmp.application.requestedAmount AS requested_amount,
    COALESCE(json_tmp.application.requestedAmountWithoutDiscount) AS requested_amount_without_discount,
    COALESCE(json_tmp.application.storeId, json_tmp.metadata.context.storeId) AS store_slug,
    COALESCE(json_tmp.metadata.context.userId, json_tmp.metadata.context.storeUserId) AS store_user_id,
    json_tmp.metadata.context.storeUserName AS store_user_name,
    -- CUSTOM ATTRIBUTES
    CAST(TRUE AS BOOLEAN) AS custom_is_checkpoint_application_legacy,
    'LEGACY' AS custom_platform_version,
    CAST(TRUE AS BOOLEAN) AS custom_is_preapproval_completed
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'prospectloanapplicationcreatedfromcheckpoint_co') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}