








--Variables
    
    
    
    
    
    
    
    
    --Conditions
    

    
        
        
        
    

    --Query
    with
    ally_vertical_tbl as (
    select slug, 
       nvl(vertical:name.value::string, 'NO_VERTICAL') as vertical
       from cur_br.allies group by slug, vertical
    ),
    source_tbl_max_dt as (
    select
    (select max(application_date) 
        from silver.f_applications_br) as source_tbl_max_dt
    ),
    ts_data_a as (
    select  date_format(from_utc_timestamp(application_date, 
        "America/Sao_Paulo"), "yyyy-MM-dd HH:00:00.000-0300") as dt_local,

    
        
        count(1) as real_metric_value

    from silver.f_applications_br app
    left join ally_vertical_tbl ally
    on ally.slug = app.ally_slug
        where application_date is not null
        and   application_date < 
          (select date_format(source_tbl_max_dt, 
          "yyyy-MM-dd HH:00:00") from source_tbl_max_dt)

        group by dt_local 
                 
        

        order by dt_local
    ),
    ts_data as (
        select  date_format(from_utc_timestamp(ran.date, "America/Sao_Paulo"), "yyyy-MM-dd HH:00:00.000-0300") as dt_local,                
                
                nvl(dat.real_metric_value, 0) as real_metric_value
            from ts_data_a dat right join gold.timeseries_date_range ran 
            on dat.dt_local = date_format(from_utc_timestamp(ran.date, "America/Sao_Paulo"), "yyyy-MM-dd HH:00:00.000-0300")
        order by dt_local
    ),
    ts_data_cum as (
    select dt_local, 
        real_metric_value,

    

        SUM(real_metric_value) OVER (PARTITION BY 
        date_format(from_utc_timestamp(dt_local, "America/Sao_Paulo"), "yyyy-MM-dd") 
        ORDER BY dt_local ROWS UNBOUNDED PRECEDING) as real_cumulative_value 
    from ts_data
    ),
    ts_data_all as (
    select 
        "br" as country,
        dt_local, 
        "hourly" as period_date,
        date_format(from_utc_timestamp( dt_local, "America/Sao_Paulo"), "HH:00:00") as period_value, 
        "number_of_applications" as metric,
        to_json( named_struct("aggregation", 
        named_struct( "type", array("country"

        
        
        ), "values", 
        array("br"

        
        
        ) )) ) as ts_details,
        real_metric_value,
        real_cumulative_value,
        date_format(current_timestamp, "yyyy-MM-dd HH:mm:ss.000Z") as occurred_on

    from ts_data_cum
)

    select country, 
        dt_local,
        period_date,
        period_value,
        metric,
        ts_details,
        real_metric_value, 
        real_cumulative_value,
        occurred_on

    from ts_data_all
        where
        to_utc_timestamp(dt_local, 'UTC') < 
        (select date_format(source_tbl_max_dt, "yyyy-MM-dd HH:00:00") from source_tbl_max_dt)

    
        and to_utc_timestamp(dt_local, 'UTC') between 
            ("2022-01-01" - INTERVAL 1 DAY) and ("2022-01-30" + INTERVAL 1 DAY)
    