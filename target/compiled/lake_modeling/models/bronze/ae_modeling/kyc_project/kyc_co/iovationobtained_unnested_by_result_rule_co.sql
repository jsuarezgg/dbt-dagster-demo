
--raw_modeling.iovationobtained_co
WITH select_explode AS (
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
        EXPLODE(json_tmp.iovation.data.details.ruleResults.rules) AS item,
        -- CUSTOM ATTRIBUTES
        'V1' AS custom_kyc_event_version
        -- CAST(ocurred_on AS TIMESTAMP) AS iovationobtained_co_at -- To store it as a standalone column, when needed
    FROM  raw_modeling.iovationobtained_co
    -- DBT INCREMENTAL SENTENCE

    
        WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
    

)
SELECT
    -- ITEM FIELDS
    CONCAT('EID_',event_id,'_IPI_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A')) AS surrogate_key,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A') AS item_pseudo_idx,
    -- MANDATORY FIELDS
    event_name_original,
    event_name,
    event_id,
    application_id,
    client_id,
    'json_tmp.iovation.data.details.ruleResults.rules._ARRAY_' AS array_parent_path,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    -- SPECIAL DOUBLE ARRAYS - ITEM ARRAYS
    -- ITEM INNER FIELDS
    item.reason AS iovation_rule_result_reason,
    item.score AS iovation_rule_result_score,
    item.type AS iovation_rule_result_type,
    -- CUSTOM ATTRIBUTES
    custom_kyc_event_version

FROM select_explode