


--raw_modeling.creditcheckpsychometricpassedbr_br
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
      -- Fill with your mapped fields
    json_tmp.metadata.context.allyId as ally_slug,
    json_tmp.application.id as application_id,
    json_tmp.client.id as client_id,
    coalesce(cast(json_tmp.creditCheck.lowBalanceLoan as boolean),
             cast(json_tmp.creditCheck.lowBalanceLoanV2 as boolean)) as lbl,
    json_tmp.creditCheck.learningPopulation as learning_population,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(false AS BOOLEAN) as returning_client
-- DBT SOURCE REFERENCE
from raw_modeling.creditcheckpsychometricpassedbr_br
-- DBT INCREMENTAL SENTENCE

