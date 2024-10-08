


--raw_modeling.identityphotoscompleted_br
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
    json_tmp.identityVerification.agentUserId as agent_user_id,
    json_tmp.application.id as application_id,
    json_tmp.metadata.context.ipAddress.value as ip_address,
    json_tmp.identityVerification.isHighRisk as is_high_risk,
    json_tmp.metadata.context.isMobileDevice.value as is_mobile,
    json_tmp.identityVerification.observations as observations,
    json_tmp.identityVerification.status.reason as reason,
    json_tmp.identityVerification.usedPolicyId as used_policy_id,
    json_tmp.metadata.context.userAgent.value as user_agent
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw_modeling.identityphotoscompleted_br
-- DBT INCREMENTAL SENTENCE

