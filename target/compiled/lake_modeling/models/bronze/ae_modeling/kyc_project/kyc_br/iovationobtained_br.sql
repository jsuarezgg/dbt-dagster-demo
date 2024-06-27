
--raw_modeling.iovationobtained_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS
    COALESCE(json_tmp.applicationId, json_tmp.iovation.applicationId) AS application_id,
    COALESCE(json_tmp.prospectId, json_tmp.iovation.prospectId) AS client_id,
    json_tmp.iovation.blackBox AS iovation_blackBox,
    json_tmp.iovation.data.details.device.alias AS iovation_data_details_device_alias,
    json_tmp.iovation.data.details.device.blackboxMetadata.age AS iovation_data_details_device_blackboxMetadata_age,
    TO_TIMESTAMP(json_tmp.iovation.data.details.device.blackboxMetadata.timestamp) AS iovation_data_details_device_blackboxMetadata_timestamp,
    json_tmp.iovation.data.details.device.browser.configuredLanguage AS iovation_data_details_device_browser_configuredLanguage,
    json_tmp.iovation.data.details.device.browser.cookiesEnabled AS iovation_data_details_device_browser_cookiesEnabled,
    json_tmp.iovation.data.details.device.browser.language AS iovation_data_details_device_browser_language,
    json_tmp.iovation.data.details.device.browser.timezone AS iovation_data_details_device_browser_timezone,
    json_tmp.iovation.data.details.device.browser.type AS iovation_data_details_device_browser_type,
    json_tmp.iovation.data.details.device.browser.version AS iovation_data_details_device_browser_version,
    TO_TIMESTAMP(json_tmp.iovation.data.details.device.firstSeen) AS iovation_data_details_device_firstSeen,
    json_tmp.iovation.data.details.device.isNew AS iovation_data_details_device_isNew,
    json_tmp.iovation.data.details.device.os AS iovation_data_details_device_os,
    json_tmp.iovation.data.details.device.screen AS iovation_data_details_device_screen,
    json_tmp.iovation.data.details.device.type AS iovation_data_details_device_type,
    json_tmp.iovation.data.details.realIp.address AS iovation_data_details_realIp_address,
    json_tmp.iovation.data.details.realIp.ipLocation.city AS iovation_data_details_realIp_ipLocation_city,
    json_tmp.iovation.data.details.realIp.ipLocation.country AS iovation_data_details_realIp_ipLocation_country,
    json_tmp.iovation.data.details.realIp.ipLocation.countryCode AS iovation_data_details_realIp_ipLocation_countryCode,
    json_tmp.iovation.data.details.realIp.ipLocation.latitude AS iovation_data_details_realIp_ipLocation_latitude,
    json_tmp.iovation.data.details.realIp.ipLocation.longitude AS iovation_data_details_realIp_ipLocation_longitude,
    json_tmp.iovation.data.details.realIp.ipLocation.region AS iovation_data_details_realIp_ipLocation_region,
    json_tmp.iovation.data.details.realIp.isp AS iovation_data_details_realIp_isp,
    json_tmp.iovation.data.details.realIp.parentOrganization AS iovation_data_details_realIp_parentOrganization,
    json_tmp.iovation.data.details.realIp.source AS iovation_data_details_realIp_source,
    json_tmp.iovation.data.details.ruleResults.rulesMatched AS iovation_data_details_ruleResults_rulesMatched,
    json_tmp.iovation.data.details.ruleResults.score AS iovation_data_details_ruleResults_score,
    json_tmp.iovation.data.details.statedIp.address AS iovation_data_details_statedIp_address,
    json_tmp.iovation.data.details.statedIp.ipLocation.city AS iovation_data_details_statedIp_ipLocation_city,
    json_tmp.iovation.data.details.statedIp.ipLocation.country AS iovation_data_details_statedIp_ipLocation_country,
    json_tmp.iovation.data.details.statedIp.ipLocation.countryCode AS iovation_data_details_statedIp_ipLocation_countryCode,
    json_tmp.iovation.data.details.statedIp.ipLocation.latitude AS iovation_data_details_statedIp_ipLocation_latitude,
    json_tmp.iovation.data.details.statedIp.ipLocation.longitude AS iovation_data_details_statedIp_ipLocation_longitude,
    json_tmp.iovation.data.details.statedIp.ipLocation.region AS iovation_data_details_statedIp_ipLocation_region,
    json_tmp.iovation.data.details.statedIp.isp AS iovation_data_details_statedIp_isp,
    json_tmp.iovation.data.details.statedIp.parentOrganization AS iovation_data_details_statedIp_parentOrganization,
    json_tmp.iovation.data.details.statedIp.source AS iovation_data_details_statedIp_source,
    json_tmp.iovation.data.id AS iovation_data_id,
    json_tmp.iovation.data.reason AS iovation_data_reason,
    json_tmp.iovation.data.result AS iovation_data_result,
    json_tmp.iovation.data.trackingNumber AS iovation_data_trackingNumber,
    TO_TIMESTAMP(json_tmp.iovation.requestedAt) AS iovation_requestedAt,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS iovationobtained_br_at -- To store it as a standalone column, when needed
FROM  raw_modeling.iovationobtained_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
