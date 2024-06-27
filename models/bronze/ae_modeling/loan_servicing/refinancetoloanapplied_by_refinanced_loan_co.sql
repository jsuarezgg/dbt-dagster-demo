
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.refinancetoloanapplied_co
WITH explode_items as (
    SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.clientId AS client_id,
    json_tmp.loanId AS condoned_by_origination_of_loan_id,
    explode(json_tmp.refinancedLoans) AS refinanced_loan
    -- CUSTOM ATTRIBUTES
     -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS refinancetoloanapplied_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'refinancetoloanapplied_co') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)


SELECT 
    CONCAT('EID_',event_id,'_RLID_',COALESCE(refinanced_loan.condonation.loanId,refinanced_loan.loandId)) AS surrogate_key,
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	COALESCE(client_id,refinanced_loan.condonation.clientId) AS client_id,
    --- START OF ORDER REFACTOR
    refinanced_loan.condonation.id AS condonation_id,
    COALESCE(refinanced_loan.condonation.loanId,refinanced_loan.loandId) AS loan_id,
    refinanced_loan.condonation.amount AS condonation_amount,
    refinanced_loan.condonation.bucket AS condonation_bucket,
    TO_TIMESTAMP(refinanced_loan.condonation.date) AS condonation_date,
    refinanced_loan.condonation.reason AS condonation_reason,
    refinanced_loan.condonation.type AS condonation_type,
    condoned_by_origination_of_loan_id
    --- END OF ORDER REFACTOR
FROM explode_items