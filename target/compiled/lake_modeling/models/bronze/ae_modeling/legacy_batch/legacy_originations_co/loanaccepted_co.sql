


--raw_modeling.loanaccepted_co
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
    json_tmp.applicationId.id AS application_id,
    json_tmp.prospectId.id AS client_id,
    -- CUSTOM ATTRIBUTES
    CAST(1 AS TINYINT) AS custom_loan_acceptance_passed,
    named_struct(
        'event_id', json_tmp.eventId,
        'event_type', json_tmp.eventType,
        'ocurred_on', ocurred_on::TIMESTAMP,
        'acceptance',
            named_struct(
             "otp", json_tmp.otp.code
            )
    ) AS loan_acceptance_detail_json
-- DBT SOURCE REFERENCE
FROM raw_modeling.loanaccepted_co
-- DBT INCREMENTAL SENTENCE

    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
