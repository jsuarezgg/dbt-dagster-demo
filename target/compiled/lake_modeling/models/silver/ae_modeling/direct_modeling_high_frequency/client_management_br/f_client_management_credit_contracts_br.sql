


SELECT
    -- MANDATORY FIELDS
    client_id,
    loan_id,
    storage_location,
    loan_origination_date,
    loan_ownership,
    ingested_at,
    updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM bronze.client_management_credit_contracts_br AS credit_contracts