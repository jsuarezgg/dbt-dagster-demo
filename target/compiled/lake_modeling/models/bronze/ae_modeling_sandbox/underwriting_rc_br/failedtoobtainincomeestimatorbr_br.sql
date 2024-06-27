


--raw_modeling.failedtoobtainincomeestimatorbr_br
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
      -- Fill with your mapped fields
    json_tmp.creditCheck.bypassRejectedReason as bypassed_reason
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw_modeling.failedtoobtainincomeestimatorbr_br
-- DBT INCREMENTAL SENTENCE

