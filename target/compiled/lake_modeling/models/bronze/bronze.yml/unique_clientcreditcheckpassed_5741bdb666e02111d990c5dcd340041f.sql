
    
    

select
    surrogate_key as unique_field,
    count(*) as n_records

from bronze.clientcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co
where surrogate_key is not null
group by surrogate_key
having count(*) > 1


