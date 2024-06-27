{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    a.application_id,
    CASE
        WHEN a.channel ILIKE '%PAY_LINK%' THEN 'PAYLINK'
        WHEN a.channel ILIKE '%E_COMMERCE%' THEN 'E_COMMERCE'
        WHEN a.channel ILIKE '%PRE%APPROVAL%' THEN 'PREAPPROVAL'
    END AS synthetic_channel
from {{ ref('f_applications_br') }} a