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


--raw_modeling.basicidentityvalidatedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.userGeolocation.clientId as client_id,
    json_tmp.userGeolocation.deviceId as device_id,
    json_tmp.userGeolocation.geolocation.altitude as altitude,
    json_tmp.userGeolocation.geolocation.altitudeAccuracy as altitude_accuracy,
    json_tmp.userGeolocation.geolocation.latitude as latitude,
    json_tmp.userGeolocation.geolocation.longitude as longitude,
    json_tmp.userGeolocation.geolocation.accuracy as geolocation_accuracy,
    json_tmp.userGeolocation.geolocation.heading as geolocation_heading,
    json_tmp.userGeolocation.geolocation.speed as geolocation_speed
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw_backend_events', 'shop_discovery_event_usergeolocationregistered_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
