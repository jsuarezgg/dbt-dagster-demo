{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- Implement manual dependencies to prevent issues like: `dbt was unable to infer all dependencies for the model
--                                              This typically happens when ref() is placed within a conditional block.`
-- depends_on: {{ ref('snp_deltas_braze_user_attributes') }}
-- depends_on: {{ ref('snp_deltas_braze_user_deletion') }}
-- depends_on: {{ ref('snp_deltas_braze_purchase_events') }}

{# STEP 1: Initialize table list variables: snapshot_delta_sources=Requested ; snapshot_delta_sources_existing=Requested & existing -#}
{%- set snapshot_delta_sources = ['snp_deltas_braze_user_attributes','snp_deltas_braze_user_deletion','snp_deltas_braze_purchase_events'] -%}
{%- set snapshot_delta_sources_existing = [] -%}

{#- STEP 2: Get the list of requested and existing tables -#}
{%- for snapshot_delta_source in snapshot_delta_sources -%}
    {%- if unity_catalog_table_exists('gold',snapshot_delta_source) -%}
        {%- do snapshot_delta_sources_existing.append(snapshot_delta_source) -%}
    {%- endif -%}
{%- endfor -%}

{#- STEP 3: Iterate over all requested and existing tables: getting its snapshot configuration and calculating the statistics over batches that have not been processed yet -#}
-- DEBUG: `snapshot_delta_sources` = {{snapshot_delta_sources}}
-- DEBUG: `snapshot_delta_sources_existing` = {{snapshot_delta_sources_existing}}
WITH
{%- for snapshot_delta_source in snapshot_delta_sources_existing %}
{%- set data_sources_config_dict = return_data_sources_config_braze(snapshot_delta_source|replace('deltas_','')) -%}
{%- set json_data_sources_config_dict = data_sources_config_dict | tojson -%}
{% if not loop.first %},{% endif %}
snapshot_source_{{snapshot_delta_source}} AS (
    SELECT *
    FROM {{ ref(snapshot_delta_source) }}
    WHERE ae_batch_id > COALESCE((SELECT MAX(ae_batch_id) FROM {{ this }} WHERE snapshot_deltas_source= '{{ snapshot_delta_source }}'),0)
)
,
batch_column_name_groupings_{{snapshot_delta_source}} AS (
--STEP 2-- ae_batch_id statistics detailed for the column_name updates.
    SELECT
        ae_batch_id,
        COLLECT_LIST(NAMED_STRUCT('column_name',column_name,'num_updates', num_updates)) AS batch_column_name_updates_array
    FROM
    ( --STEP 1-- ae_batch_id+ column_name statistic
        SELECT
            ae_batch_id,
            column_name,
            COUNT(DISTINCT dbt_scd_id) AS num_updates
        FROM snapshot_source_{{snapshot_delta_source}}
        GROUP BY 1,2
    ) --STEP 1--
    GROUP BY 1
--STEP 2--
)
,
batch_statistics_results_{{snapshot_delta_source}} AS (
--STEP 2--: Add column with detailed statistics for the column_name updates of each ae_batch_id
    SELECT
        ss.snapshot_source_batch_pairing_id,
        ss.snapshot_deltas_source,
        ss.ae_batch_id,
        ss.batch_min_timestamp,
        ss.batch_max_timestamp,
        ss.batch_num_primary_key_updates,
        ss.batch_num_delta_updates,
        cng.batch_column_name_updates_array,
        ss.statistics_runtime,
        ss.debug_snapshot_deltas_source
    FROM
    ( --STEP 1-- : Getting statistics for each ae_batch_id from this snapshot source
        SELECT
            MD5(CONCAT(COALESCE('{{ snapshot_delta_source }}'::STRING,''),'-',COALESCE(ae_batch_id::STRING,''))) AS snapshot_source_batch_pairing_id,
            '{{ snapshot_delta_source }}' AS snapshot_deltas_source,
            ae_batch_id,
            MIN(deltas_runtime) AS batch_min_timestamp,
            MAX(deltas_runtime) AS batch_max_timestamp,
            COUNT(DISTINCT {{ data_sources_config_dict.primary_key.alias }}) AS batch_num_primary_key_updates,
            COUNT(DISTINCT dbt_scd_id) AS batch_num_delta_updates,
            FIRST_VALUE(NOW()) AS statistics_runtime,
            FIRST_VALUE('{{ json_data_sources_config_dict | replace('"', '\\"') }}') AS debug_snapshot_deltas_source
        FROM snapshot_source_{{snapshot_delta_source}}
        GROUP BY 1,2,3
    ) AS ss -- snapshot_source
    --STEP 1--
    LEFT JOIN batch_column_name_groupings_{{snapshot_delta_source}} AS cng ON ss.ae_batch_id = cng.ae_batch_id
--STEP 2--
)
{% endfor %}
-- UNION All results
{#- STEP 4: UNION all partial results previously obtained for every requested and existing table -#}
{%- for snapshot_delta_source in snapshot_delta_sources %}
( SELECT * FROM batch_statistics_results_{{snapshot_delta_source}} )
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}