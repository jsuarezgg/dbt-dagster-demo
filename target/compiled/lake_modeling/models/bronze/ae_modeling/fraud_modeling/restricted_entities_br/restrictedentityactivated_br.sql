


--raw_modeling.restrictedentityactivated_br
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
    json_tmp.restrictedEntity.clientId AS client_id,
    json_tmp.restrictedEntity.createdAt AS restricted_entity_created_at,
    json_tmp.restrictedEntity.journey AS restricted_entity_journey,
    json_tmp.restrictedEntity.reason AS restricted_entity_reason,
    json_tmp.restrictedEntity.reference AS restricted_entity_reference,
    json_tmp.restrictedEntity.source AS restricted_entity_source,
    json_tmp.restrictedEntity.status AS restricted_entity_status,
    json_tmp.restrictedEntity.type AS restricted_entity_type,
    json_tmp.restrictedEntity.value AS restricted_entity_value,
    md5(cast(concat(coalesce(cast(json_tmp.restrictedEntity.type as 
    string
), ''), '-', coalesce(cast(json_tmp.restrictedEntity.value as 
    string
), '')) as 
    string
)) AS surrogate_key
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS restrictedentityactivated_br -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from raw_modeling.restrictedentityactivated_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
