



--raw.syc_loan_calculation_steps_config_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
client_id,
loan_id,
type,
version,
start_date,
end_date,
 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('2022-01-01') AS updated_at

-- DBT SOURCE REFERENCE
FROM raw.syc_loan_calculation_steps_config_co