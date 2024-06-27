







--COUNTRY CO




(
with
tbl_cities_regions as (
  select addr.application_id, reg.city_name, reg.region_name
  from cur.kyc_addresses addr
  inner join gold.expedition_city_homologation reg 
    on reg.id_exp_city = addr.id_exp_city
)
select "co" as country,
  date_format(application_date, "yyyy-MM-dd") as dt,
  date_format(application_date, "yyyy-MM-dd 00:00:00.000Z") as transaction_dt,
  nvl(app.store_name,'NO_STORE') as ally_slug,
  "transactions_summary" as metric,
  SUM(case when app.requested_amount_without_discount > 0 then app.requested_amount_without_discount 
      else app.requested_amount end) as origination_gvm,
  SUM(app.requested_amount) as origination_amount,
  count(1) as origination_quantity,
  nvl(app.application_channel,'NO_CHANNEL') as application_channel,
  nvl(reg.city_name,'NO_CITY') as city_name, 
  nvl(reg.region_name,'NO_REGION') as region_name,
  date_format(current_timestamp, "yyyy-MM-dd HH:mm:ss.000Z") as occurred_on
  from bronze.dynamic_funnel_originations_data_v2_co orig
  inner join bronze.dynamic_field_support_aggregator_applications_co app 
      on orig.application_id = app.application_id
  left join tbl_cities_regions reg 
      on reg.application_id = app.application_id

  
  where
    application_date between ("2022-01-01" - INTERVAL 3 DAY) and "2022-01-30"
  

  group by country, dt, transaction_dt, ally_slug, application_channel, city_name, region_name

)
union
--COUNTRY BR




(
with
tbl_cities_regions as (
  select  kyc.application_id, 
          kyc.city as city_name,
          reg.name as region_name,
          max(kyc.ocurred_on) ocurred_on
    from cur_br.kyc_addresses kyc
      inner join cur_br.cities cit on kyc.city = cit.name
      inner join cur_br.regions reg on cit.region_code = reg.code
  where kyc.city is not null
  group by kyc.application_id, kyc.city, reg.name
)
select "br" as country,
  date_format(application_date, "yyyy-MM-dd") as dt,
  date_format(application_date, "yyyy-MM-dd 00:00:00.000Z") as transaction_dt,
  nvl(app.store_name,'NO_STORE') as ally_slug,
  "transactions_summary" as metric,
  SUM(case when app.requested_amount_without_discount > 0 then app.requested_amount_without_discount 
      else app.requested_amount end) as origination_gvm,
  SUM(app.requested_amount) as origination_amount,
  count(1) as origination_quantity,
  nvl(app.application_channel,'NO_CHANNEL') as application_channel,
  nvl(reg.city_name,'NO_CITY') as city_name, 
  nvl(reg.region_name,'NO_REGION') as region_name,
  date_format(current_timestamp, "yyyy-MM-dd HH:mm:ss.000Z") as occurred_on
  from bronze.dynamic_funnel_originations_data_v2_br orig
  inner join bronze.dynamic_field_support_aggregator_applications_br app  
      on orig.application_id = app.application_id
  left join tbl_cities_regions reg 
      on reg.application_id = app.application_id

  
  where
    application_date between ("2022-01-01" - INTERVAL 3 DAY) and "2022-01-30"
  
group by country, dt, transaction_dt, ally_slug, application_channel, city_name, region_name
)