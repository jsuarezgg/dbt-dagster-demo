







--COUNTRY CO


(
  with
    ally_details as (
    select 
        ally_slug, 
        nvl(ally_name, 'NO_ALLY_NAME') as ally_name,
        nvl(ally_vertical:name.value::string, 'NO_VERTICAL') as ally_vertical,
        nvl(ally_brand:name.value::string, 'NO_BRAND') as ally_brand,
        store_slug,
        nvl(store_name, 'NO_STORE_NAME') as store_name       
    from silver.d_ally_management_stores_allies_co
    group by ally_slug, ally_name, ally_vertical, ally_brand, store_name, store_slug
    ),
    info_co as (
        select
            "co"  as country,
            date_format(application_date, "yyyy-MM-dd") as dt,
            date_format(application_date, "yyyy-MM-dd 00:00:00.000Z") as transaction_dt,
            app.application_id, 
            app.ally_slug,
            ally.ally_name,
            ally.ally_brand,
            nvl(app.channel, 'NO_CHANNEL') as channel, 
            date_format(app.application_date, "yyyy-MM-dd HH:mm:ss.000Z") as application_date,
            case when app.requested_amount_without_discount > 0 
                  then app.requested_amount_without_discount 
                  else app.requested_amount end as origination_gvm, --GVM
            rmt.mdf, --merchant discount fee
            round(app.requested_amount - (rmt.approved_amount * rmt.mdf), 2) as sales_after_mdf, --Sales after MDF
            app.store_slug,
            ally.store_name,
            bnpl.loan_id,
            nvl(ppd.document_expedition_city, 'NO_CITY') as city
        from silver.f_applications_co as app
        left join silver.f_origination_events_co as oe on app.application_id = oe.application_id
        left join silver.f_originations_bnpl_co as bnpl on app.application_id = bnpl.application_id
        left join silver.d_prospect_personal_data_co as ppd on app.client_id = ppd.client_id
        left join gold.risk_master_table_co as rmt on app.application_id = rmt.application_id
        left join ally_details as ally on app.store_slug = ally.store_slug and app.ally_slug = ally.ally_slug
    )
    
    select * 
    from info_co
    where application_date is not null 

    
    and
        application_date between ("2022-01-01" - INTERVAL 3 DAY) and "2022-01-30"
    
)

union

--COUNTRY BR


(
with
  ally_details as (
        select 
            ally_slug, 
            nvl(ally_name, 'NO_ALLY_NAME') as ally_name,
            nvl(ally_vertical:name.value::string, 'NO_VERTICAL') as ally_vertical,
            nvl(ally_brand:name.value::string, 'NO_BRAND') as ally_brand,
            store_slug,
            nvl(store_name, 'NO_STORE_NAME') as store_name       
        from silver.d_ally_management_stores_allies_br
        group by ally_slug, ally_name, ally_vertical, ally_brand, store_name, store_slug
    ),
    info_br as (
        select
            'br'  as country,
            date_format(application_date, "yyyy-MM-dd") as dt,
            date_format(application_date, "yyyy-MM-dd 00:00:00.000Z") as transaction_dt,
            app.application_id, 
            app.ally_slug,
            ally.ally_name,
            ally.ally_brand,
            nvl(app.channel, 'NO_CHANNEL') as channel, 
            date_format(app.application_date, "yyyy-MM-dd HH:mm:ss.000Z") as application_date,
            case when app.requested_amount_without_discount > 0 
                  then app.requested_amount_without_discount 
                  else app.requested_amount end as origination_gvm, --GVM
            rmt.mdf, --merchant discount fee
            round(app.requested_amount - (rmt.approved_amount * rmt.mdf), 2) as sales_after_mdf, --Sales after MDF
            app.store_slug,
            ally.store_name,
            bnpl.loan_id,
            nvl(ppd.birth_city, 'NO_CITY') as city
        from silver.f_applications_br as app
        left join silver.f_origination_events_br as oe on app.application_id = oe.application_id
        left join silver.f_originations_bnpl_br as bnpl on app.application_id = bnpl.application_id
        left join silver.f_originations_bnpn_br as bnpn on app.application_id = bnpn.application_id
        left join silver.d_prospect_personal_data_br as ppd on app.client_id = ppd.client_id
        left join gold.risk_master_table_br as rmt on app.application_id = rmt.application_id
        left join ally_details as ally on app.store_slug = ally.store_slug and app.ally_slug = ally.ally_slug
    )

    select *
    from info_br
    where application_date is not null 

    
    and
        application_date between ("2022-01-01" - INTERVAL 3 DAY) and "2022-01-30"
    
)