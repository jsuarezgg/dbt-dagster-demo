{{
    config(
        materialized='incremental',
        unique_key="snapshot_deltas_source||'-'||batch_id",
        incremental_strategy='append',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{% set snapshots_statistics = ['snp_braze_user_attributes'] %}


{%- for snp in snapshots_statistics %}
{%- set data_sources_config_dict = return_data_sources_config_braze(snp) -%}
{%- set json_data_sources_config_dict = data_sources_config_dict | tojson -%}
SELECT
    '{{ snp }}' AS snapshot_deltas_source,
    batch_id,
    MIN(deltas_runtime) AS batch_min_timestamp,
    MAX(deltas_runtime) AS batch_max_timestamp,
    COUNT(DISTINCT {{ data_sources_config_dict.primary_key.alias }}) AS batch_num_primary_key_updates,
    COUNT(1) AS batch_num_delta_updates,
    FIRST_VALUE(NOW()) AS statistics_runtime,
    '{{ json_data_sources_config_dict | replace('"', '\\"') }}' AS debug_snapshot_deltas_source
FROM {{ ref(snp) }}
WHERE batch_id > (SELECT MAX(batch_id) FROM {{ this }})
GROUP BY 1,2
{% if not loop.last %}UNION ALL{% endif %}
{% endfor -%}
