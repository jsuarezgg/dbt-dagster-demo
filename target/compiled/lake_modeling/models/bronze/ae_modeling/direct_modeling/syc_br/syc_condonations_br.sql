


--raw.syc_condonations_br
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

 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('2022-01-01') AS updated_at

-- DBT SOURCE REFERENCE
FROM raw.syc_condonations_br