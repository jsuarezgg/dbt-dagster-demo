


--raw_modeling.shoppingintentregistered_co
SELECT
    -- MANDATORY FIELDS
    event_type AS event_name_original,
    reverse(split(event_type,"\\."))[0] AS event_name,
    event_id AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.ally.slug as ally_slug,
    json_tmp.shoppingIntent.id as shopping_intent_id,
    json_tmp.client.id AS client_id,
    json_tmp.shoppingIntent.campaignId as campaign_id,
    json_tmp.shoppingIntent.channel as channel,
    json_tmp.metadata.context.deviceId AS device_id
    -- CUSTOM ATTRIBUTES
-- DBT SOURCE REFERENCE
FROM raw_modeling.shoppingintentregistered_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
