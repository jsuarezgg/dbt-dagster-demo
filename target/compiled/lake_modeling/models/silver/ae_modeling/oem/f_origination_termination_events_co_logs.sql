
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
applicationdeclined_co AS ( 
    SELECT *
    FROM bronze.applicationdeclined_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationexpired_co AS ( 
    SELECT *
    FROM bronze.applicationexpired_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationrestarted_co AS ( 
    SELECT *
    FROM bronze.applicationrestarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,originationunexpectederroroccurred_co AS ( 
    SELECT *
    FROM bronze.originationunexpectederroroccurred_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationclosedbynewone_co AS ( 
    SELECT *
    FROM bronze.applicationclosedbynewone_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationdeclined_co
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationexpired_co
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationrestarted_co
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM originationunexpectederroroccurred_co
    UNION ALL
    SELECT 
        application_id,client_id,event_type,journey_stage_name,ocurred_on,
    event_name,
    event_id
    FROM applicationclosedbynewone_co
    
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
this: silver.f_origination_termination_events_co_logs
country: co
silver_table_name: f_origination_termination_events_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'event_type', 'journey_stage_name', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'applicationdeclined': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'applicationexpired': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'applicationrestarted': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'originationunexpectederroroccurred': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}, 'applicationclosedbynewone': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'journey_stage_name', 'event_type', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['applicationdeclined', 'applicationexpired', 'applicationrestarted', 'originationunexpectederroroccurred', 'applicationclosedbynewone']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
