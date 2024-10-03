{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
{% set statistics_table_exists = unity_catalog_table_exists('gold_staging','stg_braze_snapshots_deltas_statistics') %}
{% set data_sources_config_dict = return_data_sources_config_braze('snp_braze_user_deletion') %}
SELECT
    snp.dbt_scd_id,
    snp.{{'custom_' ~ data_sources_config_dict.primary_key.alias ~ '_column_name_pairing_id'}},
    snp.{{data_sources_config_dict.primary_key.alias}},
    snp.column_name,
    snp.column_value,
    NOW() AS deltas_runtime,
    {% if statistics_table_exists %}COALESCE(last_batch.last_ae_batch_id,0) + 1{% else %}1{% endif %} AS ae_batch_id
FROM {{ ref('snp_braze_user_deletion') }} AS snp
{% if statistics_table_exists %}
LEFT JOIN (
    SELECT
        MAX(ae_batch_id) AS last_ae_batch_id,
        MAX(batch_max_timestamp) AS last_batch_max_timestamp
    FROM {{ source('gold_staging', 'stg_braze_snapshots_deltas_statistics') }} -- Source to avoid dependency circuit-break
    WHERE snapshot_deltas_source = 'snp_deltas_braze_user_deletion'
    ) last_batch ON 1=1
{% endif %}
WHERE 1=1
{% if statistics_table_exists %}AND snp.dbt_valid_from > COALESCE(last_batch.last_batch_max_timestamp, TO_TIMESTAMP('2000-01-01')){% endif %}
AND snp.dbt_valid_to IS NULL
ORDER BY 3,4;
