

-- INCREMENTAL MODEL

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
    a.event_properties:['subCategoryName'] AS sub_category_name

FROM silver.f_amplitude_addi_funnel_project a

LEFT JOIN sandbox.amplitude_events_naming n
    ON a.project = n.project
    AND a.event_type = n.old_event_name

-- DBT INCREMENTAL SENTENCE

WHERE true
        AND a.event_date BETWEEN (to_date("2022-01-01" - INTERVAL "5" HOUR))
            AND to_date("2022-01-30")
        AND a.event_time BETWEEN (to_timestamp("2022-01-01" - INTERVAL "5" HOUR))
            AND to_timestamp("2022-01-30")