







-- COUNTRY CO


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
        with applications as (
            select
                count(application_id) as application_quantity,
                app.ocurred_on_date,
                nvl(app.ally_slug, 'NO_ALLY_SLUG') as ally_slug,
                nvl(app.store_slug, 'NO_STORE_SLUG') as store_slug,
                nvl(app.channel, 'NO_CHANNEL') as channel, 
                nvl(document_expedition_city, 'NO_CITY') as city,
                date_format(application_date, "yyyy-MM-dd") as dt,
                date_format(application_date, "yyyy-MM-dd 00:00:00.000Z") as transaction_dt
            from silver.f_applications_co as app
            left join silver.d_prospect_personal_data_co as ppd on app.client_id = ppd.client_id
            group by app.ocurred_on_date, app.ally_slug, app.store_slug, document_expedition_city, app.channel, dt, transaction_dt
        ),
        originations as (
            select
                count(orig.application_id) as origination_quantity,
                orig.ocurred_on_date,
                nvl(app.ally_slug, 'NO_ALLY_SLUG') as ally_slug,
                nvl(app.store_slug, 'NO_STORE_SLUG') as store_slug,
                nvl(app.channel, 'NO_CHANNEL') as channel, 
                nvl(document_expedition_city, 'NO_CITY') as city
            from silver.f_origination_events_co as orig
            inner join silver.f_applications_co as app on orig.application_id = app.application_id
            left join silver.d_prospect_personal_data_co as ppd on orig.client_id = ppd.client_id
            group by orig.ocurred_on_date, app.ally_slug, app.store_slug, document_expedition_city, app.channel
        )

        select
            "co" as country,
            date_format(app.ocurred_on_date, "yyyy-MM-dd HH:mm:ss.000Z") as ocurred_on_date,
            application_quantity,
            origination_quantity,
            app.ally_slug,
            ally.ally_name,
            ally.ally_brand,
            app.store_slug,
            ally.store_name,
            app.channel,
            app.city,
            app.transaction_dt,
            app.dt
        from applications app
        join originations orig on app.ocurred_on_date = orig.ocurred_on_date 
            and app.ally_slug = orig.ally_slug 
            and app.store_slug = orig.store_slug 
            and app.city = orig.city
            and app.channel = orig.channel
        left join ally_details as ally on app.store_slug = ally.store_slug and app.ally_slug = ally.ally_slug
    )

    select * 
    from info_co

    
    where
        ocurred_on_date between ("2022-01-01" - INTERVAL 3 DAY) and "2022-01-30"
    

)

-- COUNTRY BR


union 

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
        with applications as (
            select
                count(application_id) as application_quantity,
                app.ocurred_on_date,
                nvl(app.ally_slug, 'NO_ALLY_SLUG') as ally_slug,
                nvl(app.store_slug, 'NO_STORE_SLUG') as store_slug,
                nvl(app.channel, 'NO_CHANNEL') as channel, 
                nvl(birth_city, 'NO_CITY') as city,
                date_format(application_date, "yyyy-MM-dd") as dt,
                date_format(application_date, "yyyy-MM-dd 00:00:00.000Z") as transaction_dt
            from silver.f_applications_br as app
            left join silver.d_prospect_personal_data_br as ppd on app.client_id = ppd.client_id
            group by app.ocurred_on_date, app.ally_slug, app.store_slug, birth_city, app.channel, dt, transaction_dt
        ),
        originations as (
            select
                count(orig.application_id) as origination_quantity,
                orig.ocurred_on_date,
                nvl(app.ally_slug, 'NO_ALLY_SLUG') as ally_slug,
                nvl(app.store_slug, 'NO_STORE_SLUG') as store_slug,
                nvl(app.channel, 'NO_CHANNEL') as channel, 
                nvl(birth_city, 'NO_CITY') as city
            from silver.f_origination_events_br as orig
            inner join silver.f_applications_br as app on orig.application_id = app.application_id
            left join silver.d_prospect_personal_data_br as ppd on orig.client_id = ppd.client_id
            group by orig.ocurred_on_date, app.ally_slug, app.store_slug, birth_city, app.channel
        )

        select
            "br" as country,
            date_format(app.ocurred_on_date, "yyyy-MM-dd HH:mm:ss.000Z") as ocurred_on_date,
            application_quantity,
            origination_quantity,
            app.ally_slug,
            ally.ally_name,
            ally.ally_brand,
            app.store_slug,
            ally.store_name,
            app.channel,
            app.city,
            app.transaction_dt,
            app.dt
        from applications app
        join originations orig on app.ocurred_on_date = orig.ocurred_on_date 
            and app.ally_slug = orig.ally_slug 
            and app.store_slug = orig.store_slug 
            and app.city = orig.city
            and app.channel = orig.channel
        left join ally_details as ally on app.store_slug = ally.store_slug and app.ally_slug = ally.ally_slug
    )

    select * 
    from info_br

    
    where
        ocurred_on_date between ("2022-01-01" - INTERVAL 3 DAY) and "2022-01-30"
    
)