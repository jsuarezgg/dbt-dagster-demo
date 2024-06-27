

with condoned_loans as (
  select 
    loan_id
    ,origination_date
    ,approved_amount
    ,total_principal_paid
    ,total_applied_payment as total_payment_applied
    ,unpaid_principal
    ,total_current_principal_condoned + total_unpaid_principal_condoned + total_principal_overdue_condoned as condoned_amount
  from silver.d_snc_client_loan_calculations_co
)
--Deduce condonation date from daily_loan_status by getting first calculation_date with full condoned amount
,condonation_with_date as (
  select 
    a.loan_id
    ,min(b.calculation_date) as condonation_date
    ,min(a.condoned_amount) as condoned_amount
  from condoned_loans a 
  left join gold.dm_daily_loan_status_co b 
    on a.loan_id = b.loan_id
    and b.total_current_principal_condoned + b.total_unpaid_principal_condoned + b.total_principal_overdue_condoned >= a.condoned_amount - 1 --Substract 1 to avoid rounding problems
  where a.condoned_amount > 0
  group by 1
)
select *
from condonation_with_date