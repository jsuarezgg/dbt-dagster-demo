{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.client_management_loan_cancellation_orders_co
SELECT
    -- MANDATORY FIELDS
	loan_cancellation_id,
	loan_id,
	client_id,
	user_id,
	origination_date,
	origination_channel,
	approved_amount,
	cancellation_type,
	cancellation_date,
	cancellation_amount,
	cancellation_reason,
	is_annulled,
	annulment_reason,
	event_name,
	event_timestamp,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ ref('client_management_loan_cancellation_orders_co') }}
