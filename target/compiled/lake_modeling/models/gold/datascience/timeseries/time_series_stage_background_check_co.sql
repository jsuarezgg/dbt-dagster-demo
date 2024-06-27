




--Variables
    
    

    select
        rdt.date,
        coalesce(sum(sc.sum_stage_in), 0) as stage_in,
        coalesce(sum(sc.sum_stage_out), 0) as stage_out
    from gold.timeseries_date_range rdt
        left join gold.agg_application_id_funnel_co sc on rdt.date = sc.ocurred_on_date_hour and stage = 'background_check_co'
    where
        rdt.date < (select max(ocurred_on_date_hour) from gold.agg_application_id_funnel_co)
        
            and rdt.date between
                ("2022-01-01" - INTERVAL 1 DAY) and "2022-01-30"
        
    group by 1