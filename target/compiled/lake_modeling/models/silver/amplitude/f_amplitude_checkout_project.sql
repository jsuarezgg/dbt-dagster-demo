WITH full_source AS (SELECT 
    "Checkout" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as 
    string
), ''), '-', coalesce(cast(session_id as 
    string
), ''), '-', coalesce(cast(event_id as 
    string
), '')) as 
    string
)) AS unique_id,
    event_time,
    event_type,
    event AS bronze_table_source,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    NULL AS ally_country,
    NULL AS ally_brand_name,
    NULL AS ally_brand_slug,
    NULL AS ally_categories,
    event_properties['ally.slug'] ally_slug,
    NULL AS ally_channel,
    coalesce(event_properties['ally.name'],event_properties['allyName']) ally_name,
    NULL AS ally_vertical_name,
    NULL AS ally_state,
    event_properties['applicationid'] application_id,
    event_properties['channel'] channel,
    NULL AS source,
    event_properties['client.type'] client_type,
    event_properties['policy.productType'] product_type,
    event_properties['journey.name'] journey_name,
    to_json(event_properties['journey.stages'],Map("ignoreNullFields","false")) AS journey_stages,
    event_properties['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties
FROM bronze.amplitude_checkout_originations_application_declined_co

-- DBT INCREMENTAL SENTENCE
    WHERE event_date BETWEEN to_date("2022-01-01") AND to_date("2022-01-01")
    AND event_time BETWEEN to_timestamp("2022-01-01") AND to_timestamp("2022-01-01")


)

SELECT * FROM full_source;