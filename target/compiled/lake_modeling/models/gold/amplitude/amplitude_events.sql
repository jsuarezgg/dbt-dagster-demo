





    

        select distinct
            table_schema as "table_schema",
            table_name as "table_name",
            
            case table_type
                when 'BASE TABLE' then 'table'
                when 'EXTERNAL TABLE' then 'external'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(table_type)
            end as "table_type"

        from None.information_schema.tables
        where table_schema ilike 'bronze'
        and table_name ilike '%'
        and table_name not ilike ''


    
