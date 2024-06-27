

--raw.client_payments_client_payments_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    payment_method,
    amount,
    reference_code,
    client_id,
    CAST(created_at AS TIMESTAMP) as created_at,
    CAST(payment_date AS TIMESTAMP) as payment_date,
    origination_zone,
    CAST(annulled as BOOLEAN) as annulled,
    annulment_reason,
    payment_ownership,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.client_payments_client_payments_br