

--raw_modeling.client_management_clients_statements_v2_co
SELECT
    -- MANDATORY FIELDS
    client_id,
    creation_date,
    cut_off_date,
    location_path,
    status,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from raw.client_management_clients_statements_v2_co