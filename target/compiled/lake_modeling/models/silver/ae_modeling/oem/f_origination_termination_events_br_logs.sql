
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
applicationapproved_br AS ( 
    SELECT *
    FROM bronze.applicationapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeclined_br AS ( 
    SELECT *
    FROM bronze.applicationdeclined_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationexpired_br AS ( 
    SELECT *
    FROM bronze.applicationexpired_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationrestarted_br AS ( 
    SELECT *
    FROM bronze.applicationrestarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,originationunexpectederroroccurred_br AS ( 
    SELECT *
    FROM bronze.originationunexpectederroroccurred_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationapproved_br
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationdeclined_br
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationexpired_br
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationrestarted_br
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM originationunexpectederroroccurred_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM union_bronze 
    
)   



, final AS (
    SELECT 
        *,
        date(ocurred_on ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM union_all_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_origination_termination_events_br_logs
country: br
silver_table_name: f_origination_termination_events_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'event_type', 'journey_stage_name', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'applicationapproved': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'applicationdeclined': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'applicationexpired': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'applicationrestarted': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'originationunexpectederroroccurred': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['applicationapproved', 'applicationdeclined', 'applicationexpired', 'applicationrestarted', 'originationunexpectederroroccurred']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
