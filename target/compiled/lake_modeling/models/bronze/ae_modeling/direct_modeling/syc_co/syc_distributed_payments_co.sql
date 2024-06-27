


--raw.syc_distributed_payments_co
SELECT
 -- MAPPED FIELDS - DIRECT ATTRIBUTES
 id,
 client_id,
 payment_id,
 loan_id,
 CAST(amount AS DECIMAL(28,6)) as amount,
 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('2022-01-01') AS updated_at

-- DBT SOURCE REFERENCE
FROM raw.syc_distributed_payments_co