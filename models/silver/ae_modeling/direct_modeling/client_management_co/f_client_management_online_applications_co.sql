{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.client_management_online_applications_co
SELECT
    -- MANDATORY FIELDS
    order_id,
    ally_id,
    application_id,
    detail,
    client_id,
    callback_status,
    status,
    origin,
    created_at,
    applicant_type,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('client_management_online_applications_co') }}