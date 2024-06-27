

select 
    client_id as prospect_id
    , max(personId_fullName) as prospect_full_name
    , max(personId_firstName) as prospect_first_name
    , 'null' as prospect_middle_name
    , max(personId_lastName) as prospect_last_name
    , 'null' as prospect_snd_last_name
    --, 1 rn
from silver.f_kyc_bureau_personal_info_br
where personId_fullName is not null
group by 1