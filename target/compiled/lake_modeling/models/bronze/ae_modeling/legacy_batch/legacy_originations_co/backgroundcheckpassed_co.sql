


--raw_modeling.backgroundcheckpassed_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.applicationId,
    COALESCE(json_tmp.application.applicationId.id,
             json_tmp.loanApplication.applicationId.id) AS application_id,
    COALESCE(json_tmp.application.prospect.id.id,
             json_tmp.loanApplication.prospect.id.id) AS client_id,
    COALESCE(json_tmp.metadata.context.userId, json_tmp.metadata.context.storeUserId) AS store_user_id
    -- CUSTOM ATTRIBUTES

-- DBT SOURCE REFERENCE
from raw_modeling.backgroundcheckpassed_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
