


--raw_modeling.identitywarejected_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.application.product AS product,
    json_tmp.identityVerification.agentUserId as agent_user_id,
    json_tmp.identityVerification.observations as observations,
    json_tmp.identityVerification.status.reason as reason, 
    json_tmp.identityVerification.usedPolicyId as used_policy_id,
    -- CUSTOM ATTRIBUTES
    'V2' as custom_idv_version,
    CAST(1 AS TINYINT) AS idv_failed,
    CAST(ocurred_on AS TIMESTAMP) AS identitywarejected_co_at, -- To store it as a standalone column, when needed
    CAST(ocurred_on AS TIMESTAMP) AS identitywarejected_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from raw_modeling.identitywarejected_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
