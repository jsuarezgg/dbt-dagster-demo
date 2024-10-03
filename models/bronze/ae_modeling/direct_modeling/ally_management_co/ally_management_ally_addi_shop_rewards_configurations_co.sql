{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_ally_addi_shop_rewards_configurations_co
SELECT
    id AS configuration_id,
    ally_slug,
    status AS configuration_status,
    eligibility_list_id,
    data:rewards AS rewards_config_dict,
    data:creationDate AS configuration_creation_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_ally_addi_shop_rewards_configurations_co') }}
