


--raw_modeling.prospectidentityverificationinitialmessageresponsearrived_co
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
    json_tmp.prospectIdentityVerification.applicationId.value as application_id,
    json_tmp.prospectIdentityVerification.prospect.id.value as client_id,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(ocurred_on AS TIMESTAMP) AS prospectidentityverificationinitialmessageresponsearrived_at,
    'V1' as custom_idv_version
-- DBT SOURCE REFERENCE
from raw_modeling.prospectidentityverificationinitialmessageresponsearrived_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
