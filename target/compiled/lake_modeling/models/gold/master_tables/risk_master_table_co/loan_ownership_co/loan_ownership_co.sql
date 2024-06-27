

with loanreturnedfromtrust_co AS (
  select
    *,
    row_number() over (
      partition by loan_id
      order by
        ocurred_on desc
    ) as rn
  from
    bronze.loanreturnedfromtrust_co
  having
    rn = 1
  order by
    loan_id,
    rn asc
)

,loansoldtotrust_co as (
  select
    *,
    row_number() over (
      partition by loan_id
      order by
        sold_on desc,
        ocurred_on desc
    ) as rn
  from
    bronze.loansoldtotrust_co
  order by
    loan_id,
    sold_on desc,
    rn asc
),
loantransfernotificationsent_co as (
  select
    *,
    row_number() over (
      partition by loan_id
      order by
        ocurred_on desc
    ) as rn
  from
    bronze.loantransfernotificationsent_co
  order by
    loan_id,
    ocurred_on desc,
    rn asc
),
credit_contracts as (
  select
    ls.loan_id,
    ls.client_id,
    coalesce(lrt.loan_ownership, lr.loan_ownership) as loan_ownership
  from
    silver.d_syc_loan_status_co ls
    LEFT JOIN loansoldtotrust_co as lr on ls.loan_id = lr.loan_id
    LEFT JOIN loanreturnedfromtrust_co as lrt on ls.loan_id = lrt.loan_id
)
select
  distinct cmcc.loan_id,
  coalesce(cc.loan_ownership,cmcc.loan_ownership) as loan_ownership,
  cl.id_number as client_id_number,
  co.client_ownership,
  --this is due to #manual_inci_inc_22833
  case
    when lt.sold_on = '2022-08-07' then from_utc_timestamp(lt.ocurred_on, 'America/Bogota') :: date
    else coalesce(
      lt.sold_on,
      from_utc_timestamp(lt.ocurred_on, 'America/Bogota') :: date
    )
  end as sale_date
from
  silver.f_client_management_credit_contracts_co as cmcc
  left join credit_contracts cc on cmcc.loan_id = cc.loan_id
  left join silver.d_client_management_clients_co as cl on cmcc.client_id = cl.client_id
  left join loansoldtotrust_co lt on cmcc.loan_id = lt.loan_id
  and lt.rn = 1
  left join loantransfernotificationsent_co ltns on cmcc.loan_id = ltns.loan_id
  and ltns.rn = 1
  left join (
    select
      client_id,
      case
        when (
          count(*) filter (
            where
              loan_ownership = 'PA_ADDI'
          ) > 0
        ) then 'PA_ADDI'
        else 'ADDI'
      end as client_ownership
    from
      credit_contracts cc
    group by
      client_id
  ) co on cmcc.client_id = co.client_id
where
  cc.loan_ownership in ('PA_ADDI_GOLDMAN', 'PA_ADDI_ARCHITECT', 'ADDI')