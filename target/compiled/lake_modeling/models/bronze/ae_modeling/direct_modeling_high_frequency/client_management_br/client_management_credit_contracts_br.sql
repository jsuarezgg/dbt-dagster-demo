

--raw_modeling.client_management_credit_contracts_br
SELECT
    -- MANDATORY FIELDS
    client_id,
    loan_id,
    storage_location,
    loan_origination_date,
    loan_ownership,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw.client_management_credit_contracts_br