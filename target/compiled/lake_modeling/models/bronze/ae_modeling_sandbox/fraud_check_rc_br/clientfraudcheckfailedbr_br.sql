


--raw_modeling.clientfraudcheckfailedbr_br
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
    json_tmp.application.id as application_id,
    json_tmp.fraudCheck.policy as credit_policy_name,
    json_tmp.fraudCheck.status as credit_status,
    json_tmp.fraudCheck.statusReason as credit_status_reason,
    json_tmp.fraudCheck.modelScore as fraud_model_score,
    json_tmp.fraudCheck.modelVersion as fraud_model_version,
    coalesce(json_tmp.client.id,
             json_tmp.metadata.context.clientId) as client_id
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw_modeling.clientfraudcheckfailedbr_br
-- DBT INCREMENTAL SENTENCE

