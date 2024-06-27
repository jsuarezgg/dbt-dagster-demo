


--raw_modeling.allycmsintegrationmonitoringupdated_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    json_tmp.ally.slug AS ally_slug,
    json_tmp.monitoring.checkout.integration.status AS monitoring_checkout_integration_status,
    TO_TIMESTAMP(json_tmp.monitoring.checkout.integration.updatedAt) AS monitoring_checkout_integration_updated_at,
    json_tmp.monitoring.checkout.traffic.status AS monitoring_checkout_traffic_status,
    TO_TIMESTAMP(json_tmp.monitoring.checkout.traffic.updatedAt) AS monitoring_checkout_traffic_updated_at,
    json_tmp.monitoring.status AS monitoring_status,
    TO_TIMESTAMP(json_tmp.monitoring.updatedAt) AS monitoring_updated_at,
    json_tmp.monitoring.widget.integration.status AS monitoring_widget_integration_status,
    TO_TIMESTAMP(json_tmp.monitoring.widget.integration.updatedAt) AS monitoring_widget_integration_updated_at,
    json_tmp.monitoring.widget.traffic.status AS monitoring_widget_traffic_status,
    TO_TIMESTAMP(json_tmp.monitoring.widget.traffic.updatedAt) AS monitoring_widget_traffic_updated_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS allycmsintegrationmonitoringupdated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from raw_modeling.allycmsintegrationmonitoringupdated_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
