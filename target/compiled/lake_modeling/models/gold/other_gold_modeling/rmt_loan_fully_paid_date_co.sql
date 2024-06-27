

WITH cur_daily_loan_status AS (

  SELECT *
  FROM gold.dm_daily_loan_status_co
),

fully_paid_date as (
      select loan_id,
       min(calculation_date) as fully_paid_date
  from cur_daily_loan_status
  where is_fully_paid is true
  group by 1
), loans_total as (
   select  client_id,
           loan_id,
           calculation_date,
           origination_date,
           is_fully_paid
   from cur_daily_loan_status
   where is_fully_paid is true
), loan_n_prev_fully_paids as (
    select dls.client_id,
           dls.loan_id,
           count(distinct lt.loan_id) as n_previous_fully_paid_loans
    from cur_daily_loan_status dls
    left join loans_total lt
    on dls.client_id = lt.client_id
    and lt.calculation_date < dls.origination_date
    and lt.is_fully_paid = true
    group by 1, 2
)
select fpd.loan_id,
       lpfp.client_id,
       fpd.fully_paid_date,
       lpfp.n_previous_fully_paid_loans
from fully_paid_date fpd
left join loan_n_prev_fully_paids lpfp
on fpd.loan_id = lpfp.loan_id
order by 1, 3 desc