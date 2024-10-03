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
d_pii_cases_co AS (
    SELECT *
    FROM {{ ref('d_pii_cases_co') }}
)
,
ds_braze_baseline_features AS (
    SELECT *
    FROM hive_metastore.ds.braze_baseline_features --Hard-coded due to incapacity of our dbt version to reference multiple catalogs (when retrieving metadata NOT with the model references)
)
,
addi_clients_baseline_co AS (
    -- KEY: Setting flags for all clients that has done at least one application. A flag for those who have their cupo
    --      in one of the states for which we want to delete them from braze AND also one
    --      for those who have requested their PII data to be removed (on this matter check this link for further context on what it implies (https://www.notion.so/addico/PII-Silver-table-masking-41f73e0661d345238879cfd0f3e6a4d1?pvs=4)
    --      Then created a consolidated criteria flag based on those: `complies_to_be_removed_criteria`
    SELECT
        client_id,
        is_in_pii_removal,
        has_cupo_status_for_deletion,
        (is_in_pii_removal OR has_cupo_status_for_deletion) AS complies_to_be_removed_criteria
    FROM (
        SELECT
            a.client_id AS client_id,
            COALESCE(MAX(pii.client_id IS NOT NULL),FALSE) AS is_in_pii_removal,
            COALESCE(MAX(COALESCE(dsb.cupo_status,'EMPTY') IN ('CANCELED')),FALSE) AS has_cupo_status_for_deletion
        FROM      dm_applications AS a
        LEFT JOIN ds_braze_baseline_features AS dsb ON a.client_id = dsb.client_id
        LEFT JOIN (SELECT DISTINCT client_id FROM d_pii_cases_co WHERE NULLIF(NULLIF(client_id,''),'-') IS NOT NULL) AS pii ON a.client_id = pii.client_id
        WHERE a.country_code = 'CO'
        GROUP BY 1
    )
)

SELECT
    -- Full context: Braze Customer Engagement Platform - Data Scope: https://www.notion.so/addico/Braze-Customer-Engagement-Platform-Data-Scope-7502aa4eaa954b47a6e84f81b026e23d?pvs=4
    b.client_id,
    -- A. Snapshot hard-filter flags
    TRUE AS ae_complies_basic_criteria, -- For now track everything, maybe in the future limit this to existing in braze through the proxy
    -- B. Tracked values
    b.complies_to_be_removed_criteria,
    b.is_in_pii_removal,
    b.has_cupo_status_for_deletion,
    -- C. Debugging the key flags above
    NULL::STRING AS debug_ae_complies_basic_criteria, -- Casted to string to prevent issues with type inference, in the future this will be an struct field
    -- D. DATA PLATFORM DATA
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM addi_clients_baseline_co AS b