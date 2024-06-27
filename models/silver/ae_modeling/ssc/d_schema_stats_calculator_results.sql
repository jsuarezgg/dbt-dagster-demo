{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.schema_stats_calculator_results
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    uuid,
    runtime,
    country,
    delta_table,
    event_full_name,
    query_type,
    element,
    idx,
    type,
    subtype,
    exemplar_value_as_str,
    overall_num_elements,
    overall_num_elements_sum_array,
    overall_num_elements_avg_array,
    overall_min_timestamp,
    overall_max_timestamp,
    count_not_null,
    count_bad_not_null_strings,
    count_not_null_array_wise,
    min_timestamp_not_null,
    max_timestamp_not_null,
    path_dot_support,
    idx_group,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze', 'schema_stats_calculator_results') }}
