
    
    

select
    surrogate_key as unique_field,
    count(*) as n_records

from bronze.clientcreditcheckpassed_unnested_by_loan_proposal_event_br
where surrogate_key is not null
group by surrogate_key
having count(*) > 1


