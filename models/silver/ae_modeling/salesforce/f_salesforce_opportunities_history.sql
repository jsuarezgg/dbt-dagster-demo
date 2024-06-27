{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    oh.opportunity_history_id,
    oh.opportunity_id,
    oh.opportunity_history_amount,
    oh.opportunity_history_stage_name,
    oh.opportunity_history_close_date,
    oh.opportunity_history_created_date,
    oh.opportunity_history_systemmodstamp,
    oh.opportunity_history_expected_revenue,
    oh.opportunity_history_forecast_category,
    oh.opportunity_history_created_by_id,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    oh.airbyte_emitted_at,
    oh.ingested_from_s3_at
from {{ ref('salesforce_opportunity_history')}} oh