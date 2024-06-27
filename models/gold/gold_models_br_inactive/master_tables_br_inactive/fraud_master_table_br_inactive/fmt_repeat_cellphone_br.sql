{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

select
pd1.client_id,
pd1.application_id,
pd1.application_cellphone,
pd1.application_date,
count(pd2.client_id) as repeats
from {{ ref('fmt_bureau_check_br') }} pd1
left join {{ ref('fmt_bureau_check_br') }} pd2 on pd1.application_cellphone = pd2.application_cellphone and pd1.client_id != pd2.client_id and pd1.application_date > pd2.application_date
where pd1.application_cellphone is not null
and pd2.application_cellphone is not null
and pd1.application_cellphone != '0000000000'
and pd2.application_cellphone != '0000000000'
group by 1,2,3,4
