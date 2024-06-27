WITH full_source AS (SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'additional_information_br' AS bronze_table_source
FROM amplitude2.additional_information_br

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_download_csv' AS bronze_table_source
FROM amplitude2.app_ap_download_csv

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_init' AS bronze_table_source
FROM amplitude2.app_ap_init

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_init_paylink' AS bronze_table_source
FROM amplitude2.app_ap_init_paylink

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_logout' AS bronze_table_source
FROM amplitude2.app_ap_logout

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_menu_clicked' AS bronze_table_source
FROM amplitude2.app_ap_menu_clicked

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_paylink_created' AS bronze_table_source
FROM amplitude2.app_ap_paylink_created

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_paylink_failure' AS bronze_table_source
FROM amplitude2.app_ap_paylink_failure

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_paylink_reset' AS bronze_table_source
FROM amplitude2.app_ap_paylink_reset

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_paylink_submited' AS bronze_table_source
FROM amplitude2.app_ap_paylink_submited

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_store_selected' AS bronze_table_source
FROM amplitude2.app_ap_store_selected

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_ap_tx_filter_applied' AS bronze_table_source
FROM amplitude2.app_ap_tx_filter_applied

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_get_logrocket_session' AS bronze_table_source
FROM amplitude2.app_get_logrocket_session

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_init_checkout' AS bronze_table_source
FROM amplitude2.app_init_checkout

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_logout' AS bronze_table_source
FROM amplitude2.app_logout

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_navbar_button_tapped' AS bronze_table_source
FROM amplitude2.app_navbar_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_screen_opened' AS bronze_table_source
FROM amplitude2.app_screen_opened

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'app_service_error' AS bronze_table_source
FROM amplitude2.app_service_error

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    CAST(NULL AS STRING) AS ally_brand_name,
    CAST(NULL AS STRING) AS ally_brand_slug,
    CAST(NULL AS STRING) AS ally_categories,
    CAST(NULL AS STRING) AS ally_slug,
    CAST(NULL AS STRING) AS ally_channel,
    CAST(NULL AS STRING) AS ally_name,
    CAST(NULL AS STRING) AS ally_vertical_name,
    CAST(NULL AS STRING) AS ally_state,
    CAST(NULL AS STRING) AS application_id,
    CAST(NULL AS STRING) AS channel,
    CAST(NULL AS STRING) AS source,
    CAST(NULL AS STRING) AS client_type,
    CAST(NULL AS STRING) AS product_type,
    CAST(NULL AS STRING) AS journey_name,
    CAST(NULL AS STRING) AS journey_stages,
    CAST(NULL AS STRING) AS journey_current_stage,
    CAST(NULL AS STRING) AS event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'application_backgrounded' AS bronze_table_source
FROM amplitude2.application_backgrounded

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'application_change_empty_event' AS bronze_table_source
FROM amplitude2.application_change_empty_event

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'application_installed' AS bronze_table_source
FROM amplitude2.application_installed

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'application_opened' AS bronze_table_source
FROM amplitude2.application_opened

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'auth_get_pre_approved_button_tapped' AS bronze_table_source
FROM amplitude2.auth_get_pre_approved_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'auth_login_failed' AS bronze_table_source
FROM amplitude2.auth_login_failed

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'auth_login_resended_otp' AS bronze_table_source
FROM amplitude2.auth_login_resended_otp

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'auth_login_success' AS bronze_table_source
FROM amplitude2.auth_login_success

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'auth_pre_approved_failed' AS bronze_table_source
FROM amplitude2.auth_pre_approved_failed

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'auth_registration_send_code_tapped' AS bronze_table_source
FROM amplitude2.auth_registration_send_code_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'background_check_co' AS bronze_table_source
FROM amplitude2.background_check_co

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'banking_license_partner_br' AS bronze_table_source
FROM amplitude2.banking_license_partner_br

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'banking_license_partner_br_select_download_receipt' AS bronze_table_source
FROM amplitude2.banking_license_partner_br_select_download_receipt

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'device_information' AS bronze_table_source
FROM amplitude2.device_information

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'device_information_info_updated' AS bronze_table_source
FROM amplitude2.device_information_info_updated

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'discounts_ally_deal_tapped' AS bronze_table_source
FROM amplitude2.discounts_ally_deal_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'discounts_deals_category_tapped' AS bronze_table_source
FROM amplitude2.discounts_deals_category_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'discounts_home_deals_see_all_tapped' AS bronze_table_source
FROM amplitude2.discounts_home_deals_see_all_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'down_payment_br_select_copy_pix' AS bronze_table_source
FROM amplitude2.down_payment_br_select_copy_pix

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    CAST(NULL AS STRING) AS user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    CAST(NULL AS STRING) AS ally_brand_name,
    CAST(NULL AS STRING) AS ally_brand_slug,
    CAST(NULL AS STRING) AS ally_categories,
    CAST(NULL AS STRING) AS ally_slug,
    CAST(NULL AS STRING) AS ally_channel,
    CAST(NULL AS STRING) AS ally_name,
    CAST(NULL AS STRING) AS ally_vertical_name,
    CAST(NULL AS STRING) AS ally_state,
    CAST(NULL AS STRING) AS application_id,
    CAST(NULL AS STRING) AS channel,
    CAST(NULL AS STRING) AS source,
    CAST(NULL AS STRING) AS client_type,
    CAST(NULL AS STRING) AS product_type,
    CAST(NULL AS STRING) AS journey_name,
    CAST(NULL AS STRING) AS journey_stages,
    CAST(NULL AS STRING) AS journey_current_stage,
    CAST(NULL AS STRING) AS event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_client_arrived' AS bronze_table_source
FROM amplitude2.home_client_arrived

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_cupo_ultra_frozen_message' AS bronze_table_source
FROM amplitude2.home_cupo_ultra_frozen_message

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_featured_section_ally_tapped' AS bronze_table_source
FROM amplitude2.home_featured_section_ally_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_featured_section_see_all_tapped' AS bronze_table_source
FROM amplitude2.home_featured_section_see_all_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_featured_section_seen' AS bronze_table_source
FROM amplitude2.home_featured_section_seen

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_first_cupo_increased_message' AS bronze_table_source
FROM amplitude2.home_first_cupo_increased_message

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_promoted_banner_seen' AS bronze_table_source
FROM amplitude2.home_promoted_banner_seen

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_promoted_banner_tapped' AS bronze_table_source
FROM amplitude2.home_promoted_banner_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_recently_visited_ally_tapped' AS bronze_table_source
FROM amplitude2.home_recently_visited_ally_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_recently_visited_section_seen' AS bronze_table_source
FROM amplitude2.home_recently_visited_section_seen

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_screen_opened' AS bronze_table_source
FROM amplitude2.home_screen_opened

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_see_all_categories_button_tapped' AS bronze_table_source
FROM amplitude2.home_see_all_categories_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_see_all_stores_button_tapped' AS bronze_table_source
FROM amplitude2.home_see_all_stores_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_select_cupo_check' AS bronze_table_source
FROM amplitude2.home_select_cupo_check

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_settings_button_tapped' AS bronze_table_source
FROM amplitude2.home_settings_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'home_store_tapped' AS bronze_table_source
FROM amplitude2.home_store_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'loan_acceptance_co' AS bronze_table_source
FROM amplitude2.loan_acceptance_co

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'notifications_pop_up_dismissed' AS bronze_table_source
FROM amplitude2.notifications_pop_up_dismissed

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'notifications_pop_up_taped' AS bronze_table_source
FROM amplitude2.notifications_pop_up_taped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'notifications_push_notification_taped' AS bronze_table_source
FROM amplitude2.notifications_push_notification_taped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'originations_application_declined' AS bronze_table_source
FROM amplitude2.originations_application_declined

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'originations_email_is_filled' AS bronze_table_source
FROM amplitude2.originations_email_is_filled

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'originations_email_is_valid' AS bronze_table_source
FROM amplitude2.originations_email_is_valid

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'originations_issue_date_filled_filled' AS bronze_table_source
FROM amplitude2.originations_issue_date_filled_filled

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'originations_mobile_phone_edited' AS bronze_table_source
FROM amplitude2.originations_mobile_phone_edited

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_br_pix_copy_code_button_tapped' AS bronze_table_source
FROM amplitude2.payments_br_pix_copy_code_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_br_pix_form_paynow_button_touched' AS bronze_table_source
FROM amplitude2.payments_br_pix_form_paynow_button_touched

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_br_pix_pay_different_value_button_tapped' AS bronze_table_source
FROM amplitude2.payments_br_pix_pay_different_value_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_br_status_paid_out' AS bronze_table_source
FROM amplitude2.payments_br_status_paid_out

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_br_transaction_success' AS bronze_table_source
FROM amplitude2.payments_br_transaction_success

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_continue_button_tapped' AS bronze_table_source
FROM amplitude2.payments_co_continue_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_corresponsal_continue_failed' AS bronze_table_source
FROM amplitude2.payments_co_corresponsal_continue_failed

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_corresponsal_download_receipt_tapped' AS bronze_table_source
FROM amplitude2.payments_co_corresponsal_download_receipt_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_method_selected' AS bronze_table_source
FROM amplitude2.payments_co_method_selected

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_pse_continue_button_tapped' AS bronze_table_source
FROM amplitude2.payments_co_pse_continue_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_pse_continue_failed' AS bronze_table_source
FROM amplitude2.payments_co_pse_continue_failed

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_saved_payment_tapped' AS bronze_table_source
FROM amplitude2.payments_co_saved_payment_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_co_transaction_success' AS bronze_table_source
FROM amplitude2.payments_co_transaction_success

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_history_tapped' AS bronze_table_source
FROM amplitude2.payments_history_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_info_button_tapped' AS bronze_table_source
FROM amplitude2.payments_info_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_pay_button_tapped' AS bronze_table_source
FROM amplitude2.payments_pay_button_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_purchase_history_tapped' AS bronze_table_source
FROM amplitude2.payments_purchase_history_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'payments_tab_selected' AS bronze_table_source
FROM amplitude2.payments_tab_selected

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    CAST(NULL AS STRING) AS device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    CAST(NULL AS STRING) AS city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'pdp_script_script_started' AS bronze_table_source
FROM amplitude2.pdp_script_script_started

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    CAST(NULL AS STRING) AS device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    CAST(NULL AS STRING) AS city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'pdp_script_widget_inserted' AS bronze_table_source
FROM amplitude2.pdp_script_widget_inserted

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'privacy_policy_co' AS bronze_table_source
FROM amplitude2.privacy_policy_co

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'privacy_policy_co_policy_accepted' AS bronze_table_source
FROM amplitude2.privacy_policy_co_policy_accepted

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'settings_notification_modal_tapped' AS bronze_table_source
FROM amplitude2.settings_notification_modal_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'settings_notification_toggle_switched' AS bronze_table_source
FROM amplitude2.settings_notification_toggle_switched

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'shop_category_tapped' AS bronze_table_source
FROM amplitude2.shop_category_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'shop_search_bar_not_found' AS bronze_table_source
FROM amplitude2.shop_search_bar_not_found

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'shop_search_bar_tapped' AS bronze_table_source
FROM amplitude2.shop_search_bar_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'shop_select_remove_search_result' AS bronze_table_source
FROM amplitude2.shop_select_remove_search_result

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'shop_store_tapped' AS bronze_table_source
FROM amplitude2.shop_store_tapped

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    CAST(NULL AS STRING) AS version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'terms_ap_terms_accepted' AS bronze_table_source
FROM amplitude2.terms_ap_terms_accepted

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')

UNION ALL


SELECT DISTINCT
    "ADDI Funnel" AS project,
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
    REPLACE(event_type, ".", "_") AS event_type,
    session_id,
    event_id,
    user_id,
    device_id,
    platform,
    device_type,
    version_name,
    country,
    city,
    to_json(user_properties,Map("ignoreNullFields","false")):['allycountry'] ally_country,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.name'] ally_brand_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.brand.slug'] ally_brand_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.categories'] AS ally_categories,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.slug'] ally_slug,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.channel'] ally_channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['ally.name'],to_json(event_properties,Map("ignoreNullFields","false") ):['allyName']) ally_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.vertical.name'] ally_vertical_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['ally.allyState'] ally_state,
    to_json(event_properties,Map("ignoreNullFields","false")):['applicationId'] application_id,
    to_json(event_properties,Map("ignoreNullFields","false")):['channel'] channel,
    coalesce(to_json(event_properties,Map("ignoreNullFields","false") ):['source'], to_json(user_properties,Map("ignoreNullFields","false") ):['source']) source,
    to_json(event_properties,Map("ignoreNullFields","false")):['client.type'] client_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['policy.productType'] product_type,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.name'] journey_name,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.stages'] AS journey_stages,
    to_json(event_properties,Map("ignoreNullFields","false")):['journey.currentStage.name'] journey_current_stage,
    to_json(event_properties,Map("ignoreNullFields","false")) event_properties,
    to_json(user_properties,Map("ignoreNullFields","false")) user_properties,
    'underwriting_co' AS bronze_table_source
FROM amplitude2.underwriting_co

-- DBT INCREMENTAL SENTENCE

WHERE to_date(event_time) >= to_date('{start_date}') AND to_date(event_time) <= to_date('{end_date}')


)

SELECT * FROM full_source;