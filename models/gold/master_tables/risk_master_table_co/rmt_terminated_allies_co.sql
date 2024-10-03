{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH terminated_allies AS (
    SELECT ally_slug
    FROM {{ ref('d_ally_management_allies_co') }}
    WHERE active_ally = false
),
last_day_terminated_allies AS (
    SELECT app.ally_slug,
           FIRST_VALUE(app.application_date::DATE) OVER (
                PARTITION BY app.ally_slug
                ORDER BY app.application_date DESC
           ) AS last_day
    FROM {{ source('silver_live', 'f_applications_co') }} app
    INNER JOIN terminated_allies al ON app.ally_slug = al.ally_slug
),
consolidated AS (
SELECT ally_slug,
       MIN(last_day) AS ally_terminated_date
FROM last_day_terminated_allies
GROUP BY 1
)

select * from consolidated;
