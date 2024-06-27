
    
    

select
    truora_event_validation_id as unique_field,
    count(*) as n_records

from silver.f_identity_photos_third_party_truora_events_co
where truora_event_validation_id is not null
group by truora_event_validation_id
having count(*) > 1


