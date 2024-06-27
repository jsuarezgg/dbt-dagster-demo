

--raw.syc_client_payments_br
SELECT
    -- DIRECT MODELING FIELDS
    id as payment_id,
    payment_method,
    amount,
    reference_code,
    client_id,
    payment_date,
    origination_zone,
    payment_ownership,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.syc_client_payments_br