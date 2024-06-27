
    
    

select
    event_id as unique_field,
    count(*) as n_records

from silver.f_applications_br_logs
where event_id is not null
group by event_id
having count(*) > 1


