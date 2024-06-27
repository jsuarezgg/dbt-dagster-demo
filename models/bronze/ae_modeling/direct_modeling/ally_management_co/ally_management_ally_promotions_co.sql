{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.ally_management_ally_promotions_co
SELECT
    COALESCE(id,data:id) AS promotion_id,
    COALESCE(ally_slug,data:ally.slug) AS ally_slug,
    TO_TIMESTAMP(COALESCE(created_at,data:createdAt)) AS promotion_created_at,
    TO_TIMESTAMP(COALESCE(start_date,data:startDate)) AS promotion_start_date,
    TO_TIMESTAMP(COALESCE(end_date,data:endDate)) AS promotion_end_date,
    COALESCE(status,data:status) AS promotion_status,
    COALESCE(type,data:type) AS promotion_type,
    COALESCE((show_in_app)::boolean,(data:showInApp)::boolean) AS promotion_show_in_app,
    data:category AS promotion_category,
    data:imageURL AS promotion_image_url,
    data:name AS promotion_name,
    (data:upTo)::boolean AS promotion_up_to,
    data:url AS promotion_url,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'ally_management_ally_promotions_co') }}