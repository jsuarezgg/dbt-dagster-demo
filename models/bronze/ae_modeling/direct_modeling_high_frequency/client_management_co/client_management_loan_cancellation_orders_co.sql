{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_management_loan_cancellation_orders_co
SELECT
    -- MANDATORY FIELDS
	id AS loan_cancellation_id,
	loan_id,
	client_id,
	user_id,
	origination_date::TIMESTAMP AS origination_date,
	channel AS origination_channel,
	approved_amount::DOUBLE AS approved_amount,
	cancellation_type::STRING AS cancellation_type,
	cancellation_date::TIMESTAMP AS cancellation_date,
	cancellation_amount::DOUBLE AS cancellation_amount,
	cancellation_reason,
	annulled::BOOLEAN AS is_annulled,
	annulment_reason::STRING AS annulment_reason,
	event_name,
	event_timestamp::TIMESTAMP AS event_timestamp,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'client_management_loan_cancellation_orders_co') }}
