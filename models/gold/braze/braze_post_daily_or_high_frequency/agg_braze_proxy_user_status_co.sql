{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
WITH
dm_applications AS (
    SELECT *
    FROM {{ ref('dm_applications') }}
)
,

agg_braze_cdi_user_attributes AS (
    SELECT *
    FROM {{ source('gold','agg_braze_cdi_user_attributes') }}
)
,
agg_braze_cdi_user_deletion AS (
    SELECT *
    FROM {{ source('gold','agg_braze_cdi_user_deletion') }}
)
,
agg_braze_cdi_user_attributes_to_proxy AS (
    SELECT
        external_id AS client_id,
        MIN(updated_at::TIMESTAMP) AS braze_first_update_proxy_timestamp,
        MAX(updated_at::TIMESTAMP) AS braze_last_update_proxy_timestamp,
        COUNT(1) AS num_times_in_de_batches,
        COUNT(DISTINCT updated_at) AS num_times_in_unique_de_batches
    FROM agg_braze_cdi_user_attributes
    GROUP BY 1
)
,
agg_braze_cdi_user_deletion_to_proxy AS (
    SELECT
        external_id AS client_id,
        MIN(updated_at::TIMESTAMP) AS braze_first_deletion_proxy_timestamp,
        MAX(updated_at::TIMESTAMP) AS braze_last_deletion_proxy_timestamp,
        COUNT(1) AS num_times_in_de_batches,
        COUNT(DISTINCT updated_at) AS num_times_in_unique_de_batches
    FROM agg_braze_cdi_user_deletion
    GROUP BY 1
)
,
dm_applications_clients_co AS (
    SELECT
        client_id
    FROM dm_applications
    WHERE country_code = 'CO'
    GROUP BY 1
)

SELECT
    a.client_id,
    ua.braze_first_update_proxy_timestamp,
    ua.braze_last_update_proxy_timestamp,
    ud.braze_first_deletion_proxy_timestamp,
    ud.braze_last_deletion_proxy_timestamp,
    COALESCE(ua.braze_last_update_proxy_timestamp > ud.braze_last_deletion_proxy_timestamp OR (ua.braze_last_update_proxy_timestamp IS NOT NULL AND ud.braze_last_deletion_proxy_timestamp IS NULL),FALSE) AS is_on_braze_proxy,
    -- C. Debugging the key flags above
    STRUCT(ua.num_times_in_de_batches, ua.num_times_in_unique_de_batches) AS debug_user_attributes,
    STRUCT(ud.num_times_in_de_batches, ud.num_times_in_unique_de_batches) AS debug_user_deletion,
    -- D. DATA PLATFORM DATA
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM      dm_applications_clients_co             AS a
LEFT JOIN agg_braze_cdi_user_attributes_to_proxy AS ua ON a.client_id = ua.client_id
LEFT JOIN agg_braze_cdi_user_deletion_to_proxy   AS ud ON a.client_id = ud.client_id