
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


--raw_modeling.storehealthcheckreceived_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    COALESCE(NULLIF(TRIM(json_tmp.healthcheck.allySlug),''), 'UNKNOWN') AS ally_slug,
    json_tmp.healthcheck.checkoutMethods AS healthcheck_checkout_methods,
    json_tmp.healthcheck.checkoutPosition AS healthcheck_checkout_position,
    json_tmp.healthcheck.component AS healthcheck_component,
    TO_TIMESTAMP(json_tmp.healthcheck.datetime) AS healthcheck_datetime
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS storehealthcheckreceived_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'storehealthcheckreceived_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
