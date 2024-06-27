
    
    

select
    application_id as unique_field,
    count(*) as n_records

from silver.f_identity_photos_third_party_co
where application_id is not null
group by application_id
having count(*) > 1


