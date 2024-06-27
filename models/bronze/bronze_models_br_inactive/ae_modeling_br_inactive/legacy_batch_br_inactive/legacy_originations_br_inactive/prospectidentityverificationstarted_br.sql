
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


--raw_modeling.prospectidentityverificationstarted_br
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
    json_tmp.prospectIdentityVerification.applicationId.value as application_id,
    json_tmp.prospectIdentityVerification.prospect.id.value as client_id,
    json_tmp.prospectIdentityVerification.provider as idv_provider,
    json_tmp.metadata.context.ipAddress.value as ip_address,
    json_tmp.metadata.context.isMobileDevice.value as is_mobile,
    json_tmp.metadata.context.userAgent.value as user_agent,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(ocurred_on AS TIMESTAMP) AS prospectidentityverificationstarted_at,
    'V1' as custom_idv_version
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'prospectidentityverificationstarted_br') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
