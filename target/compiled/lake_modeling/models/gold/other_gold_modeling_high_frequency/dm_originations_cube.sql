

Select "CO" as country 
,from_utc_timestamp(ob.origination_date,"America/Bogota") origination_date
,ob.approved_amount
,ob.ally_slug
,ob.lbl
,ob.term
,app.client_type
,hour(ob.origination_date) as hour
,minute(ob.origination_date) as minute
,app.journey_name
,app.channel
,app.product
,ob.store_slug
,app.custom_is_santander_branched santander_branched
,ob.custom_is_santander_originated santander_origination
,"BNPL" as transaction_type
,NOW() AS ingested_at
from silver.f_originations_bnpl_co ob
left join silver.f_applications_co app on app.application_id=ob.application_id

UNION

Select "BR" as country  
,from_utc_timestamp(ob.origination_date,"America/Sao_Paulo") origination_date
,ob.approved_amount
,ob.ally_slug
,ob.lbl
,ob.term
,app.client_type
,hour(ob.origination_date) as hour
,minute(ob.origination_date) as minute
,app.journey_name
,app.channel
,app.product
,ob.store_slug
,null as santander_branched
,null as santander_origination
,"BNPL" as transaction_type
,NOW() AS ingested_at
from silver.f_originations_bnpl_br ob
left join silver.f_applications_br app on app.application_id=ob.application_id

UNION

Select "BR" as country  
,from_utc_timestamp(ob.origination_date,"America/Sao_Paulo") origination_date
,ob.requested_amount as approved_amount
,ob.ally_slug
,null as lbl
,null as term
,app.client_type
,hour(ob.origination_date) as hour
,minute(ob.origination_date) as minute
,app.journey_name
,app.channel
,app.product
,app.store_slug
,null as santander_branched
,null as santander_origination
,"BNPN" as transaction_type
,NOW() AS ingested_at
from silver.f_originations_bnpn_br ob
left join silver.f_applications_br app on app.application_id=ob.application_id