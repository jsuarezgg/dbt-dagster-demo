


SELECT
    -- MANDATORY FIELDS
    client_id,
    creation_date,
    cut_off_date,
    concat('https://addi-statements-prod.s3.amazonaws.com/' , location_path) AS location_path,
    status,
    ingested_at,
    updated_at
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM bronze.client_management_clients_statements_v2_co AS clients_statements