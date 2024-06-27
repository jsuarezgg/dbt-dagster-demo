


--raw_modeling.loanguaranteepublishedtofund_co
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
    json_tmp.loan.id.value as loan_id,
    -- CUSTOM ATTRIBUTES
    CAST(TRUE AS BOOLEAN) AS custom_to_fga,
    md5(cast(concat(coalesce(cast(event_id as 
    string
), ''), '-', coalesce(cast(json_tmp.loan.id.value as 
    string
), '')) as 
    string
)) AS surrogate_key
-- DBT SOURCE REFERENCE
FROM raw_modeling.loanguaranteepublishedtofund_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
