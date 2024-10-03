{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH pre_filtered_pd1 AS (
    SELECT client_id, application_id, lower(application_email) as application_email, application_date
    FROM {{ ref('fmt_bureau_check_co') }}
    WHERE application_email IS NOT NULL
)
, pre_filtered_pd2 AS (
    SELECT client_id, lower(application_email) as application_email, application_date
    FROM {{ ref('fmt_bureau_check_co') }}
    WHERE application_email IS NOT NULL
)
SELECT
    pd1.client_id,
    pd1.application_id,
    pd1.application_email,
    pd1.application_date,
    COUNT(pd2.client_id) AS repeats
FROM pre_filtered_pd1 pd1
LEFT JOIN pre_filtered_pd2 pd2 ON pd1.application_email = pd2.application_email AND pd1.client_id != pd2.client_id AND pd1.application_date > pd2.application_date
GROUP BY 1,2,3,4;