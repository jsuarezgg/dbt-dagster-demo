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

SELECT
    event_type AS event_name_original,
    reverse(split(event_type,"\\."))[0] AS event_name,
    event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    to_date(ocurred_on) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    json_tmp.metadata.context.traceId AS trace_id,
    json_tmp.metadata.context.clientId AS client_id,
    json_tmp.request.thirdParty AS third_party,
    json_tmp.request.serviceName AS service_name,
    json_tmp.request.method AS method,
    json_tmp.request.url AS url,
    json_tmp.request.headers AS headers,
    json_tmp.request.body AS body,
    json_tmp.request.formAttributes AS form_attributes
FROM {{ source('raw_backend_events', 'http_event_httprequestsent_co') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "5" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "5" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
