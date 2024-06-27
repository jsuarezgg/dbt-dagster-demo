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
    event_name_original,
    event_name,
    event_id,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    trace_id,
    client_id,
    third_party,
    service_name,
    method,
    url,
    headers,
    body,
    form_attributes
FROM {{ ref('httprequestsent_prod_br') }}

{% if is_incremental() %}
    WHERE ocurred_on_date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "5" HOUR)) AND to_date("{{ var('end_date') }}")
    AND ocurred_on BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "5" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
