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
    {{ bureau_report_experian_co_fields() }}
-- DBT SOURCE REFERENCE
from {{ source('raw', 'experian_report_rep_202302_co') }}