
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

--raw_modeling.applicationcreated_co
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
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.ally.slug AS ally_slug,
    explode(json_tmp.application.previousIds) AS previous_ids,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    'ORIGINATIONS_V2' AS custom_platform_version
    -- CAST(ocurred_on AS TIMESTAMP) AS applicationcreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'applicationcreated_co') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)


SELECT 
    CONCAT('EID_',event_id,'_PRVID_', previous_ids) AS surrogate_key,
    MD5(CONCAT(application_id, previous_ids)) AS custom_application_reference_id, -- capture compound key: single row for every app + previous id's combination
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	application_id,
	client_id,
	ally_slug,
	previous_ids AS previous_id
FROM explode_items