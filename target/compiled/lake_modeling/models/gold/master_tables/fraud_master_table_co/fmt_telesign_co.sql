

select 
  application_id,
  telesignPhoneId_status_description as ts_status_description,
  telesignPhoneId_simSwap_swapDateTime as ts_sim_swap_date,
  telesignPhoneId_carrier_name as ts_carrier_name
from silver.f_kyc_telesign_phoneid_v1v2_co