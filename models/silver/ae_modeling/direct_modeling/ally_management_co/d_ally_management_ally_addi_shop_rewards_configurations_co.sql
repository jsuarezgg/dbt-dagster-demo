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
    configuration_id,
    ally_slug,
    configuration_status,
    eligibility_list_id,
    rewards_config_dict,
    configuration_creation_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_addi_shop_rewards_configurations_co') }}
