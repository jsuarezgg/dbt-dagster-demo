


--raw_modeling.identityphotosthirdpartydiscardedbyrisk_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.originationEventType AS event_type,
    json_tmp.metadata.context.allyId AS ally_slug, -- alternative: json_tmp.metadata.context.allyId
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.product AS product,
    json_tmp.client.type AS client_type,
    json_tmp.application.channel AS channel,
    json_tmp.identityVerification.provider AS idv_provider,
    json_tmp.identityVerification.thirdParty.attempts AS idv_third_party_attempts,
    json_tmp.identityVerification.agentUserId as agent_user_id,
    coalesce(json_tmp.application.device.ipAddress,
             json_tmp.metadata.context.ipAddress.value) as ip_address,
    json_tmp.identityVerification.isHighRisk as is_high_risk,
    json_tmp.metadata.context.isMobileDevice.value as is_mobile,
    json_tmp.identityVerification.observations as observations,
    json_tmp.identityVerification.status.reason as reason,
    json_tmp.identityVerification.usedPolicyId as used_policy_id,
    coalesce(json_tmp.application.device.userAgent,
             json_tmp.metadata.context.userAgent.value) as user_agent,
    -- CUSTOM ATTRIBUTES
    'V4' as custom_idv_version,
    'DISCARDED_BY_RISK' AS identity_photos_tp_custom_last_status,
    CAST(ocurred_on AS TIMESTAMP) AS identityphotosthirdpartydiscardedbyrisk_co_at,
    CAST(ocurred_on AS TIMESTAMP) AS identityphotosthirdpartydiscardedbyrisk_at
-- DBT SOURCE REFERENCE
from raw_modeling.identityphotosthirdpartydiscardedbyrisk_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
