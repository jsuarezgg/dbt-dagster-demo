



SELECT
    id AS payment_id,
    payment_method,
    amount,
    reference_code,
    client_id,
    created_at,
    payment_date,
    origination_zone,
    annulled,
    annulment_reason,
    payment_ownership,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.client_payments_client_payments_br