
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


--raw_modeling.clientloanapplicationcreated_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    json_tmp.clientLoanApplication.allyId.slug as ally_slug,
    COALESCE(json_tmp.clientLoanApplication.applicationId.id, json_tmp.clientLoanApplication.applicationId.value, json_tmp.metadata.context.traceId) AS application_id,
    json_tmp.clientLoanApplication.client.cellPhoneNumber as application_cellphone,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS application_date,
    json_tmp.clientLoanApplication.client.email as application_email,
    json_tmp.clientLoanApplication.client.id.id as client_id,
    json_tmp.clientLoanApplication.client.idNumber as id_number,
    json_tmp.clientLoanApplication.client.idType as id_type,
    json_tmp.clientLoanApplication.requestedAmount as requested_amount,
    json_tmp.clientLoanApplication.requestedAmountWithoutDiscount as requested_amount_without_discount,
    json_tmp.clientLoanApplication.storeId.slug as store_slug,
    json_tmp.clientLoanApplication.user.id.value as store_user_id,
    json_tmp.metadata.context.storeUserName as store_user_name,
    -- CUSTOM ATTRIBUTES
    CAST(TRUE AS BOOLEAN) as custom_is_returning_client_legacy,
    'V1' as custom_idv_version,
    'LEGACY' AS custom_platform_version
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'clientloanapplicationcreated_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
