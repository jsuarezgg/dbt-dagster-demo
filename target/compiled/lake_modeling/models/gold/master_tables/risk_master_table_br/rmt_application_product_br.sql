

select 
  application_id,
  case
    when journey_name ilike '%preap%' then 'PREAPPROVAL_BR'
    when application_date <= '2021-05-31' then 'PAGO_BR'
    when (journey_name ilike '%bnpn%' or a.custom_is_bnpn_branched = True) then 'BNPN_BR'
    else 'PAGO_BR'
  end as product
from silver.f_applications_br a