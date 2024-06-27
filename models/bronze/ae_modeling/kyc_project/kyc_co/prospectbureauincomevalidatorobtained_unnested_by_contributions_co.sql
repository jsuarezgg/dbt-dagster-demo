{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--raw_modeling.prospectbureauincomevalidatorobtained_co

WITH explode_contributions AS (
    SELECT explode(json_tmp.income.contributions) contributions,
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.applicationId AS application_id,
    json_tmp.prospectId AS client_id,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
FROM {{source('raw_modeling', 'prospectbureauincomevalidatorobtained_co' )}}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)

SELECT
CONCAT('EID_',event_id,'_IPI_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A')) AS surrogate_key,
ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A') AS item_pseudo_idx,
event_name_original,
event_name,
event_id,
ocurred_on,
application_id,
client_id,
'json_tmp.income.contributions._ARRAY_' AS array_parent_path,
ocurred_on_date,
ingested_at,
updated_at,
custom_kyc_event_version,
contributions.averageIncome AS income_contribution_averageIncome,
NULLIF(TRIM(contributions.contributor.name),'') AS income_contribution_contributor_name,
contributions.contributor.type AS income_contribution_contributor_type,
contributions.hasIntegralSalary AS income_contribution_hasIntegralSalary,
contributions.type AS income_contribution_type
FROM explode_contributions
