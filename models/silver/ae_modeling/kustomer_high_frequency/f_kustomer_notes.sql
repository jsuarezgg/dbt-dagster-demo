{{
    config(
        materialized='incremental',
        unique_key='note_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 6494418 by 2023-09-19
--bronze.kustomer_notes
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    note_id,
    body,
    created_at,
    updated_at,
    modified_at,
    org_id,
    customer_id,
    created_by_id,
    modified_by_id,
    conversation_id,
    reference_date,
    reference_date_year,
    reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_notes') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updated_at) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updated_at AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}