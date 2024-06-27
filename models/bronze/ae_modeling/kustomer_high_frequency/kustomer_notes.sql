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
--raw_modeling.kus_notes
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    noteId AS note_id,
    NULLIF(TRIM(body),'') AS body,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    orgId AS org_id,
    customerId AS customer_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    conversationId AS conversation_id,
    referenceDate AS reference_date,
    year AS reference_date_year,
    month AS reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_notes') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE to_date(updatedAt) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(updatedAt AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
