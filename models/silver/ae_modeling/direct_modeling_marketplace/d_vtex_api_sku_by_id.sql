{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_sku_by_id_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
id AS sku_id,
productid AS product_id,
name AS sku_name,
activateifpossible AS activate_if_possible,
iskit AS is_kit,
rewardvalue AS reward_value,
creationdate::timestamp AS creation_date,
estimateddatearrival::date AS estimated_date_arrival,
NULLIF(TRIM(manufacturercode),'') AS manufacturer_code,
measurementunit AS measurement_unit,
unitmultiplier AS unit_multiplier,
NULLIF(TRIM(refid),'') AS ref_id,
isactive AS is_active,
videos,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze','vtex_sku_by_id_api') }}