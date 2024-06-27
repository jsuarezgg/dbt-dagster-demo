{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with journey_stages_ordered as (
select application_id,
       journey_stage_name,
       event_name,
       ocurred_on,
       row_number() over (partition by application_id order by ocurred_on asc) as rn
from {{ ref('f_origination_events_co_logs') }}
where journey_stage_name is not null
order by application_id asc, rn asc)
select application_id,
       concat_ws('|', array_distinct(collect_list(journey_stage_name))) as journey_stages,
       concat_ws('|', array_distinct(collect_list(event_name))) as stages
from journey_stages_ordered
group by 1
