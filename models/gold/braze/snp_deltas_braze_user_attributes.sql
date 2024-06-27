{{
    config(
        materialized='incremental',
        unique_key='batch_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT 
    snp.client_id,
    snp.column_name,
    snp.column_value,
    NOW() AS deltas_runtime,
    COALESCE(last_batch.last_ae_batch_id,0) + 1 AS ae_batch_id
FROM {{ ref('snp_braze_user_attributes') }} AS snp
LEFT JOIN (
    SELECT
        MAX(ae_batch_id) AS last_ae_batch_id,
        MAX(batch_max_timestamp) AS last_batch_max_timestamp
    FROM {{ source('gold_staging', 'stg_braze_snapshots_deltas_statistics') }} -- Source to avoid circuit-break
    WHERE snapshot_deltas_source = 'snp_deltas_braze_user_attributes'
    ) last_batch ON 1=1
WHERE 1=1
AND snp.dbt_valid_from > COALESCE(last_batch.last_batch_max_timestamp, TO_TIMESTAMP('2000-01-01'))
AND snp.dbt_valid_to IS NULL
ORDER BY 1,2;
