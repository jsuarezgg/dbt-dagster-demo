








    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'application_change_empty_event') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_application_change_empty_event
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_init') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_init
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_init_paylink') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_init_paylink
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_logout') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_logout
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_menu_clicked') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_menu_clicked
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_paylink_created') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_paylink_created
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_paylink_failure') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_paylink_failure
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_paylink_reset') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_paylink_reset
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_paylink_submited') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_paylink_submited
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_ap_tx_filter_applied') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_ap_tx_filter_applied
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_init_checkout') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_init_checkout
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_into_webview') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_into_webview
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_logout') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_logout
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_navbar_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_navbar_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'app_screen_opened') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_app_screen_opened
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'auth_login_failed') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_auth_login_failed
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'auth_login_resended_otp') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_auth_login_resended_otp
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'auth_registration_send_code_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_auth_registration_send_code_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'background_check_co') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_background_check_co
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'cellphone_validation_co_change_cellphone') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_cellphone_validation_co_change_cellphone
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'device_information') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_device_information
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'device_information_info_updated') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_device_information_info_updated
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_featured_section_see_all_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_featured_section_see_all_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_first_cupo_increased_message') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_first_cupo_increased_message
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_promoted_banner_seen') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_promoted_banner_seen
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_promoted_banner_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_promoted_banner_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_screen_opened') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_screen_opened
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_see_all_categories_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_see_all_categories_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_see_all_stores_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_see_all_stores_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_select_cupo_check') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_select_cupo_check
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_select_favorite_store') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_select_favorite_store
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_settings_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_settings_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'home_store_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_home_store_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_back_document_photo_uploaded') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_back_document_photo_uploaded
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_black_photos_error') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_black_photos_error
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_camera_alternatives') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_camera_alternatives
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_camera_permission_error') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_camera_permission_error
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_collected') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_collected
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_copy_url_to_clipboard') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_copy_url_to_clipboard
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_front_document_photo_uploaded') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_front_document_photo_uploaded
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_native_camera_image_loaded') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_native_camera_image_loaded
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_selfie_photo_uploaded') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_selfie_photo_uploaded
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_started') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_started
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_unsupported_browser') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_unsupported_browser
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_unsupported_desktop') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_unsupported_desktop
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'identity_photos_using_native_camera') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_identity_photos_using_native_camera
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'loan_proposals_co_grande_proposal_details_showed') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_loan_proposals_co_grande_proposal_details_showed
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'notifications_pop_up_dismissed') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_notifications_pop_up_dismissed
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'notifications_pop_up_taped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_notifications_pop_up_taped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'notifications_push_notification_taped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_notifications_push_notification_taped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'originations_application_declined') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_originations_application_declined
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'originations_email_is_filled') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_originations_email_is_filled
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'originations_email_is_valid') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_originations_email_is_valid
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'originations_issue_date_filled_filled') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_originations_issue_date_filled_filled
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'originations_mobile_phone_edited') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_originations_mobile_phone_edited
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_continue_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_continue_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_corresponsal_continue_failed') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_corresponsal_continue_failed
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_corresponsal_download_receipt_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_method_selected') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_method_selected
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_pse_continue_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_pse_continue_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_pse_continue_failed') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_pse_continue_failed
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_saved_payment_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_saved_payment_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_co_transaction_success') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_co_transaction_success
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_history_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_history_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_info_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_info_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_pay_button_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_pay_button_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_purchase_history_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_purchase_history_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_tab_selected') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_tab_selected
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'payments_web_corresponsal_info') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_payments_web_corresponsal_info
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'personal_information_co_grande_preapproval_research') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_personal_information_co_grande_preapproval_research
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'personal_information_co_grande_preapproval_research_started') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_personal_information_co_grande_preapproval_research_started
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'preapproval_survey') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_preapproval_survey
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'preapproval_survey_preapproval_survey_updated') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_preapproval_survey_preapproval_survey_updated
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'privacy_policy_v2_co_cell_phone_used') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_privacy_policy_v2_co_cell_phone_used
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_add_to_cart') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_add_to_cart
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_cart') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_cart
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_complete_purchase') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_complete_purchase
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_continue_shipping_information') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_continue_shipping_information
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_deal') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_deal
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_expand_subcategory') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_expand_subcategory
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_fav_marketplace_product') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_fav_marketplace_product
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_go_to_website') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_go_to_website
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_marketplace_banner') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_marketplace_banner
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_marketplace_department') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_marketplace_department
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_marketplace_menu') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_marketplace_menu
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_marketplace_product') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_marketplace_product
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_marketplace_seller') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_marketplace_seller
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_merchant_logo') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_merchant_logo
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_quick_add_marketplace_product') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_quick_add_marketplace_product
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_rewards_filter') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_rewards_filter
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_rewards_info') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_rewards_info
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_rewards_menu') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_rewards_menu
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_see_all_rewards_stores') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_see_all_rewards_stores
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'select_store') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_select_store
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'settings_notification_modal_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_settings_notification_modal_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'settings_notification_toggle_switched') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_settings_notification_toggle_switched
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'shop_category_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_shop_category_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'shop_search_bar_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_shop_search_bar_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'shop_select_remove_search_result') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_shop_select_remove_search_result
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'shop_store_tapped') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_shop_store_tapped
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    UNION









    SELECT
      event_time::date AS event_date,
      event_time,
      from_utc_timestamp(event_time,'America/Bogota') AS event_time_cot,
      REPLACE(REPLACE(UPPER(event_type), '.', '_'), ' ', '_') AS event_type,
      md5(cast(concat(coalesce(cast(event_time as string), ''), '-',
                        coalesce(cast(session_id as string), ''), '-',
                        coalesce(cast(event_id as string), '')) as string))
                        AS unique_id,
      amplitude_id,
      uuid,
      batch_id,
      CONCAT('amplitude_addi_funnel_', 'view_marketplace_screen') AS bronze_table_source,
      session_id,
      event_id,
      user_id,
      device_id,
      platform,
      device_type,
      version_name,
      country,
      city,
      user_properties:['allycountry'] AS ally_country,
      COALESCE(event_properties:['ally.brand.name'],
                event_properties:['store.app.ally.brand.name'],
                event_properties:['params.ally.brand.name']) AS ally_brand_name,
      COALESCE(event_properties:['ally.brand.slug'],
                event_properties:['params.ally.brand.slug'],
                event_properties:['store.app.ally.brand.slug']) AS ally_brand_slug,
      event_properties:['ally.categories'] AS ally_categories,
      COALESCE(user_properties:['allySlug'],
                event_properties:['allySlug'],
                event_properties:['ally.slug'],
                event_properties:['params.ally.slug'],
                event_properties:['store.app.ally.slug']) AS ally_slug,
      COALESCE(event_properties:['ally.channel'],
                event_properties:['params.ally.channel'],
                event_properties:['store.app.ally.channel']) AS ally_channel,
      COALESCE(event_properties:['ally.name'],
                event_properties:['allyName'],
                event_properties:['store.app.ally.name']) AS ally_name,
      COALESCE(event_properties:['ally.vertical.name'],
                event_properties:['store.app.ally.vertical.name']) AS ally_vertical_name,
      COALESCE(event_properties:['ally.allyState'],
                event_properties:['params.ally.allyState'],
                event_properties:['store.app.ally.allyState']) AS ally_state,
      NULLIF(
            COALESCE(user_properties:['applicationId'],
                    event_properties:['params.applicationId'],
                    event_properties:['applicationId'],
                    event_properties:['params.payload.applicationId'],
                    event_properties:['payload.applicationId']),
            'ORIGINATIONS'
        ) AS application_id,
      COALESCE(event_properties:["channel"],
                event_properties:["store.originations.channel"]) AS channel,
      COALESCE(user_properties:['source'],
                event_properties:['source']) AS source,
      COALESCE(event_properties:['client.type'],
                event_properties:['store.originations.client.type']) AS client_type,
      event_properties:['policy.productType'] AS product_type,
      COALESCE(event_properties:['journey.name'],
                event_properties:['store.originations.journey.name']) AS journey_name,
      COALESCE(event_properties:['journey.stages'],
                event_properties:['store.originations.journey.stages']) AS journey_stages,
      COALESCE(event_properties:['journey.currentStage.name'],
                event_properties:['store.originations.journey.currentStage.name']) AS journey_current_stage,
      event_properties,
      user_properties,
      _ingestion_at,
      month(event_time) AS _month,
      year(event_time) AS _year
    FROM bronze_amplitude_fix.amplitude_addi_funnel_view_marketplace_screen
    WHERE event_time::date >= "2022-01-01"
    AND event_time <= NOW()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY 'a' DESC) = 1

    





