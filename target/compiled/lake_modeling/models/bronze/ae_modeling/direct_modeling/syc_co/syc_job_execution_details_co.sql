


--raw.syc_job_execution_details_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
name,
execution_date,
target_items,
 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('2022-01-01') AS updated_at

-- DBT SOURCE REFERENCE
FROM raw.syc_job_execution_details_co