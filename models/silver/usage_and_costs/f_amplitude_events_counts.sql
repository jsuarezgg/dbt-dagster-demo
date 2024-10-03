{{
    config(
        materialized='incremental',
        unique_key="surrogate_key",
        incremental_strategy='merge',
        tags=["data_usage_and_costs"],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH base AS (

SELECT
    UPPER(REGEXP_REPLACE(event_type, '[^a-zA-Z0-9]', '_')) AS event_type,
    value:["event_time"]::date AS event_date
FROM {{ source('raw', 'amplitude_funnel_events_fix') }}
{% if is_incremental() -%}
WHERE 1=1
    AND `_year` >= year(to_date("{{ var('start_date') }}" - INTERVAL "3" DAY))
    AND `_month` >= month(to_date("{{ var('start_date') }}" - INTERVAL "3" DAY))
    AND value:["event_time"]::date BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "3" DAY)) AND to_date("{{ var('start_date') }}")
{%- endif %}

)

SELECT
    {{ dbt_utils.surrogate_key('event_type', 'event_date') }} AS surrogate_key,
    first_value(event_type) AS event_type,
    first_value(event_date) AS event_date,
    COUNT(1) AS count_of_events,
    first_value(NOW()) AS ingested_at,
    first_value(to_timestamp("{{ var('start_date') }}")) AS updated_at
FROM base
GROUP BY 1;
