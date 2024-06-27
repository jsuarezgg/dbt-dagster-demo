{{
    config(
        materialized='incremental',
        unique_key="unique_id",
        incremental_strategy='append',
        full_refresh = false,
        pre_hook=[
            'DELETE FROM {{ this }} WHERE true
                AND to_date(event_time) BETWEEN (to_date("{{ var("start_date") }}" - INTERVAL "5" HOUR))
                    AND (to_date("{{ var("end_date") }}" + INTERVAL "2" HOUR))
                AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var("start_date") }}" - INTERVAL "5" HOUR))
                    AND (to_timestamp("{{ var("end_date") }}" + INTERVAL "2" HOUR))',
        ],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{%- set silver_sources = [
    'f_amplitude_addi_funnel_project'
] -%}


{%- if is_incremental() %}

-- INCREMENTAL MODEL

WITH f_amplitude_addi_funnel_project AS (

SELECT
  MAX(a.project) AS project,
  MAX(a.event_date) AS event_date,
  a.unique_id,
  MAX(a.event_time) AS event_time,
  UPPER(MAX(a.event_type)) AS event_type,
  MAX(a.session_id) AS session_id,
  MAX(a.event_id) AS event_id,
  MAX(a.user_id) AS user_id,
  MAX(a.device_id) AS device_id,
  MAX(a.platform) AS platform,
  MAX(a.device_type) AS device_type,
  MAX(a.version_name) AS version_name,
  MAX(a.country) AS country,
  MAX(a.city) AS city,
  MAX(a.ally_country) AS ally_country,
  MAX(a.ally_brand_name) AS ally_brand_name,
  MAX(a.ally_brand_slug) AS ally_brand_slug,
  MAX(a.ally_categories) AS ally_categories,
  MAX(a.ally_slug) AS ally_slug,
  MAX(a.ally_channel) AS ally_channel,
  MAX(a.ally_name) AS ally_name,
  MAX(a.ally_vertical_name) AS ally_vertical_name,
  MAX(a.ally_state) AS ally_state,
  MAX(a.application_id) AS application_id,
  MAX(a.channel) AS channel,
  MAX(a.source) AS source,
  MAX(a.client_type) AS client_type,
  MAX(a.product_type) AS product_type,
  MAX(a.journey_name) AS journey_name,
  MAX(a.journey_stages) AS journey_stages,
  MAX(a.journey_current_stage) AS journey_current_stage,
  MAX(a.event_properties) AS event_properties,
  MAX(a._ingestion_at) AS _ingestion_at,
  MAX(a.amplitude_id) AS amplitude_id
FROM {{ ref('f_amplitude_addi_funnel_project') }} a
WHERE true
AND to_date(a.event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
    AND (to_date("{{ var('end_date') }}") + INTERVAL "2" HOUR)
AND to_timestamp(a.event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
    AND (to_timestamp("{{ var('end_date') }}") + INTERVAL "2" HOUR)
GROUP BY unique_id

)

SELECT
    a.project,
    a.event_date,
    a.unique_id,
    a.event_time,
    UPPER(a.event_type) AS event_type,
    CASE
        WHEN new_event_name IS NOT NULL THEN new_event_name
        ELSE event_type
    END AS event_type_holog,
    a.session_id,
    a.event_id,
    a.user_id,
    a.device_id,
    a.platform,
    a.device_type,
    a.version_name,
    a.country,
    a.city,
    a.ally_country,
    a.ally_brand_name,
    a.ally_brand_slug,
    a.ally_categories,
    a.ally_slug,
    a.ally_channel,
    a.ally_name,
    a.ally_vertical_name,
    a.ally_state,
    a.application_id,
    a.channel,
    a.source,
    a.client_type,
    a.product_type,
    a.journey_name,
    a.journey_stages,
    a.journey_current_stage,
    CASE
      WHEN (cast(a.event_properties:['screenName'] as string) ILIKE '%regalo%' OR
            cast(a.event_properties:['screenName'] as string) ILIKE '%amistad%' OR
            cast(a.event_properties:['screenName'] as string) ILIKE '%amigo%')
          THEN 'DIA_AMISTAD'
      WHEN  cast(a.event_properties:['screenName'] as string) = 'SUB_CATEGORY'
          THEN 'SUBCATEGORY'
      ELSE cast(a.event_properties:['screenName'] as string) END AS screen_name,
    COALESCE(cast(a.event_properties:['componentName'] as string), cast(a.event_properties:['origin'] as string)) AS component_name,
    a.event_properties:['categoryName'] AS category_name,
    a.event_properties:['subcategoryName'] AS sub_category_name,
    a.event_properties:['searchedWord'] AS searched_word,
    a._ingestion_at AS _ingestion_at,
    a.event_properties:['addiCupo.stateV2'] AS addi_cupo_state_v2,
    a.amplitude_id AS amplitude_id
FROM f_amplitude_addi_funnel_project a
LEFT JOIN sandbox.amplitude_events_naming n
    ON a.project = n.project
    AND a.event_type = n.old_event_name

{%- endif %}



{%- if not is_incremental() %}

-- FULL REFRESH MODEL

WITH full_source AS (

{%- for model in silver_sources %}

SELECT 
    project,
    event_date,
    unique_id,
    event_time,
    event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    ally_country,
    ally_brand_name,
    ally_brand_slug,
    ally_categories,
    ally_slug,
    ally_channel,
    ally_name,
    ally_vertical_name,
    ally_state,
    application_id,
    channel,
    source,
    client_type,
    product_type,
    journey_name,
    journey_stages,
    journey_current_stage

FROM {{ ref(model) }}

{% if not loop.last -%}
UNION ALL
{% endif -%}

{% endfor -%}
    
)

SELECT 
    a.project,
    a.event_date,
    a.unique_id,
    a.event_time,
    a.event_type,
    CASE
        WHEN new_event_name IS NOT NULL THEN new_event_name
        ELSE event_type
    END AS event_type_holog,
    a.session_id,
    a.event_id,
    a.user_id,
    a.device_id,
    a.platform,
    a.device_type,
    a.version_name,
    a.country,
    a.city,
    a.ally_country,
    a.ally_brand_name,
    a.ally_brand_slug,
    a.ally_categories,
    a.ally_slug,
    a.ally_channel,
    a.ally_name,
    a.ally_vertical_name,
    a.ally_state,
    a.application_id,
    a.channel,
    a.source,
    a.client_type,
    a.product_type,
    a.journey_name,
    a.journey_stages,
    a.journey_current_stage,
    CASE
      WHEN (cast(a.event_properties:['screenName'] as string) ILIKE '%regalo%' OR
            cast(a.event_properties:['screenName'] as string) ILIKE '%amistad%' OR
            cast(a.event_properties:['screenName'] as string) ILIKE '%amigo%')
          THEN 'DIA_AMISTAD'
      WHEN  cast(a.event_properties:['screenName'] as string) = 'SUB_CATEGORY'
          THEN 'SUBCATEGORY'
      ELSE cast(a.event_properties:['screenName'] as string) END AS screen_name,
    COALESCE(cast(a.event_properties:['componentName'] as string), cast(a.event_properties:['origin'] as string)) AS component_name,
    a.event_properties:['categoryName'] AS category_name,
    a.event_properties:['subcategoryName'] AS sub_category_name,
    a.event_properties:['searchedWord'] AS searched_word,
    a._ingestion_at AS _ingestion_at,
    a.event_properties:['addiCupo.stateV2'] AS addi_cupo_state_v2,
    a.amplitude_id AS amplitude_id
FROM full_source a

LEFT JOIN {{ source('sandbox', 'amplitude_events_naming')}} n
    ON a.project = n.project
    AND a.event_type = n.old_event_name

{%- endif %}
