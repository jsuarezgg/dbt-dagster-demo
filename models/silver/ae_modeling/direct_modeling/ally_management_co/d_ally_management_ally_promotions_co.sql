{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT 
    promotion_id,
    ally_slug,
    promotion_created_at,
    promotion_start_date,
    promotion_end_date,
    promotion_status,
    promotion_type,
    promotion_show_in_app,
    promotion_category,
    promotion_image_url,
    promotion_name,
    promotion_up_to,
    promotion_url,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_promotions_co') }}
