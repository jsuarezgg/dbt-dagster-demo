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

WITH

amplitude_addi_funnel_additional_information_br_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_additional_information_br') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_additional_information_br_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_additional_information_br' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_additional_information_br_event_base

),

amplitude_addi_funnel_app_ap_download_csv_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_download_csv') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_download_csv_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_download_csv' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_download_csv_event_base

),

amplitude_addi_funnel_app_ap_init_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_init') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_init_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_init' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_init_event_base

),

amplitude_addi_funnel_app_ap_init_paylink_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS  version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_init_paylink') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_init_paylink_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_init_paylink' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_init_paylink_event_base

),

amplitude_addi_funnel_app_ap_logout_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_logout') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_logout_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_logout' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_logout_event_base

),

amplitude_addi_funnel_app_ap_menu_clicked_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_menu_clicked') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_menu_clicked_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_menu_clicked' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_menu_clicked_event_base

),

amplitude_addi_funnel_app_ap_paylink_created_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_paylink_created') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_paylink_created_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_paylink_created' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_paylink_created_event_base

),

amplitude_addi_funnel_app_ap_paylink_failure_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_paylink_failure') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_paylink_failure_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_paylink_failure' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_paylink_failure_event_base

),

amplitude_addi_funnel_app_ap_paylink_reset_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_paylink_reset') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_paylink_reset_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_paylink_reset' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_paylink_reset_event_base

),

amplitude_addi_funnel_app_ap_paylink_submited_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_paylink_submited') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_paylink_submited_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_paylink_submited' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_paylink_submited_event_base

),

amplitude_addi_funnel_app_ap_store_selected_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_store_selected') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_store_selected_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_store_selected' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_store_selected_event_base

),

amplitude_addi_funnel_app_ap_tx_filter_applied_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_ap_tx_filter_applied') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_ap_tx_filter_applied_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_ap_tx_filter_applied' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_ap_tx_filter_applied_event_base

),

amplitude_addi_funnel_app_get_logrocket_session_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_get_logrocket_session') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_get_logrocket_session_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_get_logrocket_session' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_get_logrocket_session_event_base

),

amplitude_addi_funnel_app_init_checkout_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_init_checkout') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_init_checkout_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_init_checkout' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_init_checkout_event_base

),

amplitude_addi_funnel_app_logout_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_logout') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_logout_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_logout' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_logout_event_base

),

amplitude_addi_funnel_app_navbar_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_navbar_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_navbar_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_navbar_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_navbar_button_tapped_event_base

),

amplitude_addi_funnel_app_screen_opened_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_screen_opened') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_screen_opened_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_screen_opened' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_screen_opened_event_base

),

amplitude_addi_funnel_app_service_error_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_service_error') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_service_error_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_service_error' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_service_error_event_base

),

amplitude_addi_funnel_application_backgrounded_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_application_backgrounded') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_application_backgrounded_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_application_backgrounded' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_application_backgrounded_event_base

),

amplitude_addi_funnel_application_change_empty_event_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_application_change_empty_event') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_application_change_empty_event_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_application_change_empty_event' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_application_change_empty_event_event_base

),

amplitude_addi_funnel_application_installed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_application_installed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_application_installed_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_application_installed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_application_installed_event_base

),

amplitude_addi_funnel_application_opened_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_application_opened') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_application_opened_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_application_opened' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_application_opened_event_base

),

amplitude_addi_funnel_auth_get_pre_approved_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_auth_get_pre_approved_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_auth_get_pre_approved_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_auth_get_pre_approved_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_auth_get_pre_approved_button_tapped_event_base

),

amplitude_addi_funnel_auth_login_failed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_auth_login_failed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_auth_login_failed_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_auth_login_failed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_auth_login_failed_event_base

),

amplitude_addi_funnel_auth_login_resended_otp_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_auth_login_resended_otp') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_auth_login_resended_otp_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_auth_login_resended_otp' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_auth_login_resended_otp_event_base

),

amplitude_addi_funnel_auth_login_success_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_auth_login_success') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_auth_login_success_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_auth_login_success' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_auth_login_success_event_base

),

amplitude_addi_funnel_auth_pre_approved_failed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_auth_pre_approved_failed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_auth_pre_approved_failed_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_auth_pre_approved_failed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_auth_pre_approved_failed_event_base

),

amplitude_addi_funnel_auth_registration_send_code_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_auth_registration_send_code_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_auth_registration_send_code_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_auth_registration_send_code_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_auth_registration_send_code_tapped_event_base

),

amplitude_addi_funnel_background_check_co_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_background_check_co') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_background_check_co_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_background_check_co' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_background_check_co_event_base

),

amplitude_addi_funnel_banking_license_partner_br_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_banking_license_partner_br') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_banking_license_partner_br_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_banking_license_partner_br' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_banking_license_partner_br_event_base

),

amplitude_addi_funnel_banking_license_partner_br_select_download_receipt_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_banking_license_partner_br_select_download_receipt') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_banking_license_partner_br_select_download_receipt_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_banking_license_partner_br_select_download_receipt' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_banking_license_partner_br_select_download_receipt_event_base

),

amplitude_addi_funnel_device_information_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_device_information') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_device_information_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_device_information' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_device_information_event_base

),

amplitude_addi_funnel_device_information_info_updated_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_device_information_info_updated') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_device_information_info_updated_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_device_information_info_updated' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_device_information_info_updated_event_base

),

amplitude_addi_funnel_discounts_ally_deal_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_discounts_ally_deal_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_discounts_ally_deal_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_discounts_ally_deal_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_discounts_ally_deal_tapped_event_base

),

amplitude_addi_funnel_discounts_deals_category_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_discounts_deals_category_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_discounts_deals_category_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_discounts_deals_category_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_discounts_deals_category_tapped_event_base

),

amplitude_addi_funnel_discounts_home_deals_see_all_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_discounts_home_deals_see_all_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_discounts_home_deals_see_all_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_discounts_home_deals_see_all_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_discounts_home_deals_see_all_tapped_event_base

),

amplitude_addi_funnel_down_payment_br_select_copy_pix_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_down_payment_br_select_copy_pix') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_down_payment_br_select_copy_pix_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_down_payment_br_select_copy_pix' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_down_payment_br_select_copy_pix_event_base

),

amplitude_addi_funnel_home_client_arrived_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        CAST(NULL AS STRING) AS user_id,
        device_id,
        platform,
        device_type,
        version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_client_arrived') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_client_arrived_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_client_arrived' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_client_arrived_event_base

),

amplitude_addi_funnel_home_cupo_ultra_frozen_message_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_cupo_ultra_frozen_message') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_cupo_ultra_frozen_message_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_cupo_ultra_frozen_message' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_cupo_ultra_frozen_message_event_base

),

amplitude_addi_funnel_home_featured_section_ally_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_featured_section_ally_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_featured_section_ally_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_featured_section_ally_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_featured_section_ally_tapped_event_base

),

amplitude_addi_funnel_home_featured_section_see_all_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_featured_section_see_all_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_featured_section_see_all_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_featured_section_see_all_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_featured_section_see_all_tapped_event_base

),

amplitude_addi_funnel_home_featured_section_seen_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_featured_section_seen') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_featured_section_seen_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_featured_section_seen' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_featured_section_seen_event_base

),

amplitude_addi_funnel_home_first_cupo_increased_message_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_first_cupo_increased_message') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_first_cupo_increased_message_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_first_cupo_increased_message' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_first_cupo_increased_message_event_base

),

amplitude_addi_funnel_home_promoted_banner_seen_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_promoted_banner_seen') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_promoted_banner_seen_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_promoted_banner_seen' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_promoted_banner_seen_event_base

),

amplitude_addi_funnel_home_promoted_banner_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_promoted_banner_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_promoted_banner_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_promoted_banner_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_promoted_banner_tapped_event_base

),

amplitude_addi_funnel_home_recently_visited_ally_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_recently_visited_ally_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_recently_visited_ally_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_recently_visited_ally_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_recently_visited_ally_tapped_event_base

),

amplitude_addi_funnel_home_recently_visited_section_seen_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_recently_visited_section_seen') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_recently_visited_section_seen_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_recently_visited_section_seen' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_recently_visited_section_seen_event_base

),

amplitude_addi_funnel_home_see_all_categories_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_see_all_categories_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_see_all_categories_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_see_all_categories_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_see_all_categories_button_tapped_event_base

),

amplitude_addi_funnel_home_see_all_stores_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_see_all_stores_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_see_all_stores_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_see_all_stores_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_see_all_stores_button_tapped_event_base

),

amplitude_addi_funnel_home_select_cupo_check_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_select_cupo_check') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_select_cupo_check_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_select_cupo_check' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_select_cupo_check_event_base

),

amplitude_addi_funnel_home_settings_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_settings_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_settings_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_settings_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_settings_button_tapped_event_base

),

amplitude_addi_funnel_home_store_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_store_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_home_store_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_store_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_store_tapped_event_base

),

amplitude_addi_funnel_loan_acceptance_co_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_loan_acceptance_co') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_loan_acceptance_co_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_loan_acceptance_co' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_loan_acceptance_co_event_base

),

amplitude_addi_funnel_notifications_pop_up_dismissed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_notifications_pop_up_dismissed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_notifications_pop_up_dismissed_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_notifications_pop_up_dismissed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_notifications_pop_up_dismissed_event_base

),

amplitude_addi_funnel_notifications_pop_up_taped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_notifications_pop_up_taped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_notifications_pop_up_taped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_notifications_pop_up_taped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_notifications_pop_up_taped_event_base

),

amplitude_addi_funnel_notifications_push_notification_taped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_notifications_push_notification_taped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_notifications_push_notification_taped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_notifications_push_notification_taped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_notifications_push_notification_taped_event_base

),

amplitude_addi_funnel_originations_application_declined_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_originations_application_declined') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_originations_application_declined_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_originations_application_declined' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_originations_application_declined_event_base

),

amplitude_addi_funnel_originations_email_is_filled_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_originations_email_is_filled') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_originations_email_is_filled_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_originations_email_is_filled' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_originations_email_is_filled_event_base

),

amplitude_addi_funnel_originations_email_is_valid_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_originations_email_is_valid') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_originations_email_is_valid_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_originations_email_is_valid' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_originations_email_is_valid_event_base

),

amplitude_addi_funnel_originations_issue_date_filled_filled_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_originations_issue_date_filled_filled') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_originations_issue_date_filled_filled_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_originations_issue_date_filled_filled' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_originations_issue_date_filled_filled_event_base

),

amplitude_addi_funnel_originations_mobile_phone_edited_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_originations_mobile_phone_edited') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_originations_mobile_phone_edited_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_originations_mobile_phone_edited' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_originations_mobile_phone_edited_event_base

),

amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped_event_base

),

amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched_event_base

),

amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped_event_base

),

amplitude_addi_funnel_payments_br_status_paid_out_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_br_status_paid_out') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_br_status_paid_out_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_br_status_paid_out' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_br_status_paid_out_event_base

),

amplitude_addi_funnel_payments_br_transaction_success_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_br_transaction_success') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_br_transaction_success_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_br_transaction_success' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_br_transaction_success_event_base

),

amplitude_addi_funnel_payments_co_continue_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_continue_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_continue_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_continue_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_continue_button_tapped_event_base

),

amplitude_addi_funnel_payments_co_corresponsal_continue_failed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_corresponsal_continue_failed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_corresponsal_continue_failed_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_corresponsal_continue_failed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_corresponsal_continue_failed_event_base

),

amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped_event_base

),

amplitude_addi_funnel_payments_co_method_selected_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_method_selected') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_method_selected_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_method_selected' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_method_selected_event_base

),

amplitude_addi_funnel_payments_co_pse_continue_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_pse_continue_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_pse_continue_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_pse_continue_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_pse_continue_button_tapped_event_base

),

amplitude_addi_funnel_payments_co_pse_continue_failed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_pse_continue_failed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_pse_continue_failed_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_pse_continue_failed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_pse_continue_failed_event_base

),

amplitude_addi_funnel_payments_co_saved_payment_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_saved_payment_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_saved_payment_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_saved_payment_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_saved_payment_tapped_event_base

),

amplitude_addi_funnel_payments_co_transaction_success_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_co_transaction_success') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_co_transaction_success_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_co_transaction_success' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_co_transaction_success_event_base

),

amplitude_addi_funnel_payments_history_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_history_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_history_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_history_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_history_tapped_event_base

),

amplitude_addi_funnel_payments_info_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_info_button_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_info_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_info_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_info_button_tapped_event_base

),

amplitude_addi_funnel_payments_pay_button_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_pay_button_tapped') }}
    WHERE true   
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_pay_button_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_pay_button_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_pay_button_tapped_event_base

),

amplitude_addi_funnel_payments_purchase_history_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_purchase_history_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_purchase_history_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_purchase_history_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_purchase_history_tapped_event_base

),

amplitude_addi_funnel_payments_tab_selected_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_tab_selected') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_tab_selected_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_tab_selected' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_tab_selected_event_base

),

amplitude_addi_funnel_pdp_script_script_started_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        CAST(NULL AS STRING) AS device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        CAST(NULL AS STRING) AS city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_pdp_script_script_started') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_pdp_script_script_started_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_pdp_script_script_started' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_pdp_script_script_started_event_base

),

amplitude_addi_funnel_pdp_script_widget_inserted_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        CAST(NULL AS STRING) AS device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        CAST(NULL AS STRING) AS city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_pdp_script_widget_inserted') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_pdp_script_widget_inserted_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_pdp_script_widget_inserted' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_pdp_script_widget_inserted_event_base

),

amplitude_addi_funnel_privacy_policy_co_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_privacy_policy_co') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_privacy_policy_co_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_privacy_policy_co' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_privacy_policy_co_event_base

),

amplitude_addi_funnel_privacy_policy_co_policy_accepted_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_privacy_policy_co_policy_accepted') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_privacy_policy_co_policy_accepted_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_privacy_policy_co_policy_accepted' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_privacy_policy_co_policy_accepted_event_base

),

amplitude_addi_funnel_settings_notification_modal_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_settings_notification_modal_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_settings_notification_modal_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_settings_notification_modal_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_settings_notification_modal_tapped_event_base

),

amplitude_addi_funnel_settings_notification_toggle_switched_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_settings_notification_toggle_switched') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_settings_notification_toggle_switched_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_settings_notification_toggle_switched' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_settings_notification_toggle_switched_event_base

),

amplitude_addi_funnel_shop_category_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_shop_category_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_shop_category_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_shop_category_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_shop_category_tapped_event_base

),

amplitude_addi_funnel_shop_search_bar_not_found_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_shop_search_bar_not_found') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_shop_search_bar_not_found_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_shop_search_bar_not_found' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_shop_search_bar_not_found_event_base

),

amplitude_addi_funnel_shop_search_bar_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_shop_search_bar_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_shop_search_bar_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_shop_search_bar_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_shop_search_bar_tapped_event_base

),

amplitude_addi_funnel_shop_select_remove_search_result_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_shop_select_remove_search_result') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_shop_select_remove_search_result_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_shop_select_remove_search_result' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_shop_select_remove_search_result_event_base

),

amplitude_addi_funnel_shop_store_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_shop_store_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_shop_store_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_shop_store_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_shop_store_tapped_event_base

),

amplitude_addi_funnel_terms_ap_terms_accepted_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_terms_ap_terms_accepted') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_terms_ap_terms_accepted_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_terms_ap_terms_accepted' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_terms_ap_terms_accepted_event_base

),

amplitude_addi_funnel_underwriting_co_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_underwriting_co') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_underwriting_co_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_underwriting_co' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_underwriting_co_event_base

),

amplitude_addi_funnel_home_select_favorite_store_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_select_favorite_store') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))


),

amplitude_addi_funnel_home_select_favorite_store_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_select_favorite_store' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_select_favorite_store_event_base

),

amplitude_addi_funnel_payments_web_corresponsal_info_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_payments_web_corresponsal_info') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_payments_web_corresponsal_info_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_payments_web_corresponsal_info' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_payments_web_corresponsal_info_event_base

),

amplitude_addi_funnel_home_product_tapped_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_product_tapped') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))


),

amplitude_addi_funnel_home_product_tapped_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_product_tapped' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_product_tapped_event_base

),

amplitude_addi_funnel_home_screen_opened_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_home_screen_opened') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))


),

amplitude_addi_funnel_home_screen_opened_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_home_screen_opened' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_home_screen_opened_event_base

),

amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))


),

amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed_event_base_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed_event_base

),

amplitude_addi_funnel_identity_photos_no_face_detected_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_no_face_detected') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_no_face_detected_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_no_face_detected' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_no_face_detected_event_base

),

amplitude_addi_funnel_identity_photos_face_centralized_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_face_centralized') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_face_centralized_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_face_centralized' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_face_centralized_event_base

),

amplitude_addi_funnel_identity_photos_centralize_face_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_centralize_face') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_centralize_face_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_centralize_face' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_centralize_face_event_base

),

amplitude_addi_funnel_identity_photos_selfie_feedback_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_selfie_feedback') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_selfie_feedback_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_selfie_feedback' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_selfie_feedback_event_base

),

amplitude_addi_funnel_identity_photos_black_photos_error_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_black_photos_error') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_black_photos_error_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_black_photos_error' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_black_photos_error_event_base

),

amplitude_addi_funnel_identity_photos_unsupported_desktop_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_unsupported_desktop') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_unsupported_desktop_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_unsupported_desktop' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_unsupported_desktop_event_base

),

amplitude_addi_funnel_identity_photos_selfie_photo_uploaded_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_selfie_photo_uploaded') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_selfie_photo_uploaded_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_selfie_photo_uploaded' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_selfie_photo_uploaded_event_base

),

amplitude_addi_funnel_identity_photos_camera_permission_error_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_camera_permission_error') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_camera_permission_error_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_camera_permission_error' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_camera_permission_error_event_base

),

amplitude_addi_funnel_identity_photos_collected_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_collected') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_collected_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_collected' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_collected_event_base

),

amplitude_addi_funnel_identity_photos_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_event_base

),

amplitude_addi_funnel_identity_photos_started_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_started') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_started_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_started' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_started_event_base

),

amplitude_addi_funnel_identity_photos_front_document_photo_uploaded_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_front_document_photo_uploaded') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_front_document_photo_uploaded_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_front_document_photo_uploaded' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_front_document_photo_uploaded_event_base

),

amplitude_addi_funnel_identity_photos_back_document_photo_uploaded_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_back_document_photo_uploaded') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_back_document_photo_uploaded_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_back_document_photo_uploaded' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_back_document_photo_uploaded_event_base

),

amplitude_addi_funnel_identity_photos_unsupported_browser_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_unsupported_browser') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_unsupported_browser_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_unsupported_browser' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_unsupported_browser_event_base

),

amplitude_addi_funnel_identity_photos_copy_url_to_clipboard_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_copy_url_to_clipboard') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_copy_url_to_clipboard_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_copy_url_to_clipboard' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_copy_url_to_clipboard_event_base

),

amplitude_addi_funnel_identity_photos_confirmation_screen_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_confirmation_screen') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_confirmation_screen_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_confirmation_screen' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_confirmation_screen_event_base

),
amplitude_addi_funnel_select_store_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_store') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_store_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_store' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_store_event_base

),

amplitude_addi_funnel_select_deal_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_deal') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_deal_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_deal' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_deal_event_base

),

amplitude_addi_funnel_select_merchant_logo_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_merchant_logo') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_merchant_logo_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_merchant_logo' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_merchant_logo_event_base

),

amplitude_addi_funnel_select_go_to_website_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_go_to_website') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_go_to_website_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_go_to_website' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_go_to_website_event_base

),

amplitude_addi_funnel_select_expand_subcategory_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_expand_subcategory') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_expand_subcategory_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_expand_subcategory' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_expand_subcategory_event_base

),

amplitude_addi_funnel_select_rewards_filter_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_rewards_filter') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_rewards_filter_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_rewards_filter' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_rewards_filter_event_base

),

amplitude_addi_funnel_select_rewards_info_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_rewards_info') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_rewards_info_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_rewards_info' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_rewards_info_event_base

),

amplitude_addi_funnel_select_rewards_menu_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_rewards_menu') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_rewards_menu_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_rewards_menu' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_rewards_menu_event_base

),

amplitude_addi_funnel_select_see_all_rewards_stores_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_see_all_rewards_stores') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_see_all_rewards_stores_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_see_all_rewards_stores' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_see_all_rewards_stores_event_base

),

amplitude_addi_funnel_identity_photos_native_camera_image_loaded_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_native_camera_image_loaded') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_native_camera_image_loaded_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_native_camera_image_loaded' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_native_camera_image_loaded_event_base

),

amplitude_addi_funnel_identity_photos_using_native_camera_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_using_native_camera') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_using_native_camera_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_using_native_camera' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_using_native_camera_event_base

),

amplitude_addi_funnel_identity_photos_camera_alternatives_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_identity_photos_camera_alternatives') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_identity_photos_camera_alternatives_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    event_properties:['ally.slug'] ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_identity_photos_camera_alternatives' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_identity_photos_camera_alternatives_event_base

),

amplitude_addi_funnel_select_marketplace_department_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_marketplace_department') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_marketplace_department_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_marketplace_department' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_marketplace_department_event_base

),

amplitude_addi_funnel_select_fav_marketplace_product_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_fav_marketplace_product') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_fav_marketplace_product_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_fav_marketplace_product' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_fav_marketplace_product_event_base

),

amplitude_addi_funnel_select_marketplace_product_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_marketplace_product') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_marketplace_product_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_marketplace_product' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_marketplace_product_event_base

),

amplitude_addi_funnel_select_marketplace_banner_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        CAST(NULL AS STRING) AS user_id,
        device_id,
        platform,
        CAST(NULL AS STRING) AS device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_marketplace_banner') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_marketplace_banner_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_marketplace_banner' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_marketplace_banner_event_base

),

amplitude_addi_funnel_select_marketplace_seller_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_marketplace_seller') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_marketplace_seller_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_marketplace_seller' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_marketplace_seller_event_base

),

amplitude_addi_funnel_select_marketplace_menu_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_marketplace_menu') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_marketplace_menu_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_marketplace_menu' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_marketplace_menu_event_base

),

amplitude_addi_funnel_select_quick_add_marketplace_product_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_quick_add_marketplace_product') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_quick_add_marketplace_product_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_quick_add_marketplace_product' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_quick_add_marketplace_product_event_base

),

amplitude_addi_funnel_select_cart_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_cart') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_cart_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_cart' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_cart_event_base

),

amplitude_addi_funnel_select_add_to_cart_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_add_to_cart') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_add_to_cart_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_add_to_cart' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_add_to_cart_event_base

),

amplitude_addi_funnel_view_marketplace_screen_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_view_marketplace_screen') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_view_marketplace_screen_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_view_marketplace_screen' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_view_marketplace_screen_event_base

),

amplitude_addi_funnel_select_complete_purchase_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_complete_purchase') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_complete_purchase_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_complete_purchase' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_complete_purchase_event_base

),

amplitude_addi_funnel_select_continue_shipping_information_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_select_continue_shipping_information') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_select_continue_shipping_information_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_select_continue_shipping_information' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_select_continue_shipping_information_event_base

),

amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started_event_base

),

amplitude_addi_funnel_personal_information_co_grande_preapproval_research_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_personal_information_co_grande_preapproval_research') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_personal_information_co_grande_preapproval_research_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_personal_information_co_grande_preapproval_research' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_personal_information_co_grande_preapproval_research_event_base

),

amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated_event_base

),

amplitude_addi_funnel_preapproval_survey_event_base AS (

    SELECT DISTINCT
        event_time,
        event_type,
        session_id,
        event_id,
        user_id,
        device_id,
        platform,
        device_type,
        CAST(NULL AS STRING) AS version_name,
        country,
        city,
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_preapproval_survey') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_preapproval_survey_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_preapproval_survey' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_preapproval_survey_event_base

),

amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used_event_base

),

amplitude_addi_funnel_app_into_webview_event_base AS (

    SELECT DISTINCT
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
        user_properties,
        event_properties,
        _ingestion_at,
        amplitude_id
    FROM {{ source('bronze_amplitude_fix', 'amplitude_addi_funnel_app_into_webview') }}
    WHERE true
        AND to_date(event_time) BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_date("{{ var('end_date') }}" + INTERVAL "2" HOUR))
        AND to_timestamp(event_time) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "5" HOUR))
            AND (to_timestamp("{{ var('end_date') }}" + INTERVAL "2" HOUR))

),

amplitude_addi_funnel_app_into_webview_event_ok AS (

SELECT
    "ADDI Funnel" AS project,
    to_date(event_time) AS event_date,
    md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                    coalesce(cast(session_id as string), ''), '-',
                    coalesce(cast(event_id as string), '')) as string))
                    AS unique_id,
    CAST(event_time AS timestamp) AS event_time,
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
    user_properties:['allycountry'] ally_country,
    event_properties:['ally.brand.name'] ally_brand_name,
    event_properties:['ally.brand.slug'] ally_brand_slug,
    event_properties:['ally.categories'] AS ally_categories,
    COALESCE(user_properties:['allySlug'],
                    event_properties:['allySlug'],
                    event_properties:['ally.slug'],
                    event_properties:['params.ally.slug'],
                    event_properties:['store.app.ally.slug']) AS ally_slug,
    event_properties:['ally.channel'] ally_channel,
    COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
    COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
    event_properties:['ally.allyState'] ally_state,
    NULLIF(
        COALESCE(user_properties:['applicationId'],
                event_properties:['params.applicationId'],
                event_properties:['applicationId'],
                event_properties:['params.payload.applicationId'],
                event_properties:['payload.applicationId']),
        'ORIGINATIONS'
    ) AS application_id,
    event_properties:['channel'] channel,
    COALESCE(user_properties:['source'], event_properties:['source']) source,
    event_properties:['client.type'] client_type,
    event_properties:['policy.productType'] product_type,
    event_properties:['journey.name'] journey_name,
    event_properties:['journey.stages'] AS journey_stages,
    event_properties:['journey.currentStage.name'] journey_current_stage,
    event_properties,
    user_properties,
    'amplitude_addi_funnel_app_into_webview' AS bronze_table_source,
    _ingestion_at,
    amplitude_id
FROM amplitude_addi_funnel_app_into_webview_event_base

),

final AS (

SELECT * FROM amplitude_addi_funnel_additional_information_br_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_download_csv_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_init_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_init_paylink_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_logout_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_menu_clicked_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_paylink_created_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_paylink_failure_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_paylink_reset_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_paylink_submited_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_store_selected_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_ap_tx_filter_applied_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_get_logrocket_session_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_init_checkout_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_logout_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_navbar_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_screen_opened_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_service_error_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_application_backgrounded_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_application_change_empty_event_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_application_installed_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_application_opened_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_auth_get_pre_approved_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_auth_login_failed_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_auth_login_resended_otp_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_auth_login_success_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_auth_pre_approved_failed_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_auth_registration_send_code_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_background_check_co_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_banking_license_partner_br_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_banking_license_partner_br_select_download_receipt_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_device_information_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_device_information_info_updated_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_discounts_ally_deal_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_discounts_deals_category_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_discounts_home_deals_see_all_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_down_payment_br_select_copy_pix_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_client_arrived_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_cupo_ultra_frozen_message_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_featured_section_ally_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_featured_section_see_all_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_featured_section_seen_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_first_cupo_increased_message_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_promoted_banner_seen_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_promoted_banner_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_recently_visited_ally_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_recently_visited_section_seen_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_see_all_categories_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_see_all_stores_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_select_cupo_check_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_settings_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_store_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_loan_acceptance_co_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_notifications_pop_up_dismissed_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_notifications_pop_up_taped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_notifications_push_notification_taped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_originations_application_declined_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_originations_email_is_filled_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_originations_email_is_valid_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_originations_issue_date_filled_filled_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_originations_mobile_phone_edited_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_br_status_paid_out_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_br_transaction_success_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_continue_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_corresponsal_continue_failed_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_method_selected_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_pse_continue_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_pse_continue_failed_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_saved_payment_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_co_transaction_success_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_history_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_info_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_pay_button_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_purchase_history_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_tab_selected_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_pdp_script_script_started_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_pdp_script_widget_inserted_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_privacy_policy_co_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_privacy_policy_co_policy_accepted_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_settings_notification_modal_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_settings_notification_toggle_switched_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_shop_category_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_shop_search_bar_not_found_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_shop_search_bar_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_shop_select_remove_search_result_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_shop_store_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_terms_ap_terms_accepted_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_underwriting_co_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_select_favorite_store_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_payments_web_corresponsal_info_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_product_tapped_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_home_screen_opened_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed_event_base_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_no_face_detected_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_face_centralized_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_centralize_face_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_selfie_feedback_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_black_photos_error_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_unsupported_desktop_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_selfie_photo_uploaded_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_camera_permission_error_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_collected_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_started_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_front_document_photo_uploaded_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_back_document_photo_uploaded_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_unsupported_browser_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_copy_url_to_clipboard_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_confirmation_screen_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_store_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_deal_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_merchant_logo_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_go_to_website_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_expand_subcategory_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_rewards_filter_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_rewards_info_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_rewards_menu_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_see_all_rewards_stores_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_native_camera_image_loaded_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_using_native_camera_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_identity_photos_camera_alternatives_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_marketplace_department_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_fav_marketplace_product_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_marketplace_product_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_quick_add_marketplace_product_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_marketplace_seller_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_marketplace_banner_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_marketplace_menu_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_cart_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_view_marketplace_screen_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_add_to_cart_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_complete_purchase_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_select_continue_shipping_information_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_personal_information_co_grande_preapproval_research_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_preapproval_survey_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL 

SELECT * FROM amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

UNION ALL

SELECT * FROM amplitude_addi_funnel_app_into_webview_event_ok
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

)

SELECT * FROM final;
