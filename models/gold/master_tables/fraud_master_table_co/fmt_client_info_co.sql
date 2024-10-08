{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

select 
    client_id as prospect_id
    , max(personId_fullName) as prospect_full_name
    , max(personId_firstName) as prospect_first_name
    , max(personId_middleName) as prospect_middle_name
    , max(personId_lastName) as prospect_last_name
    , max(personId_secondLastName) as prospect_snd_last_name
    --, 1 rn
from {{ source('silver_live', 'f_kyc_bureau_personal_info_co') }}
where personId_firstName is not null
group by 1
