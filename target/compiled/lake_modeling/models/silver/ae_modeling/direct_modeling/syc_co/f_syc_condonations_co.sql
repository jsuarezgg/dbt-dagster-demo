


--raw.syc_condonations_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
client_id,
loan_id,
bucket,
percentage,
amount,
date,
reason,
agent_id,
type,
ingested_at,
updated_at

-- DBT SOURCE REFERENCE
FROM bronze.syc_condonations_co