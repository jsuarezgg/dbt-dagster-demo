
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
applicationdeviceupdated_co AS ( 
    SELECT *
    FROM bronze.applicationdeviceupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeviceinformationupdated_co AS ( 
    SELECT *
    FROM bronze.applicationdeviceinformationupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,ocurred_on,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM applicationdeviceupdated_co
    UNION ALL
    SELECT 
        application_id,ocurred_on,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM applicationdeviceinformationupdated_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,ocurred_on,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,last_event_ocurred_on_processed as ocurred_on,user_agent,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_device_information_stage_co  
    WHERE 
    silver.f_device_information_stage_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN user_agent is not null then struct(ocurred_on, user_agent) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_agent as user_agent,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    application_id
                       
           )


, final AS (
    SELECT 
        *,
        date(last_event_ocurred_on_processed ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM grouped_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_device_information_stage_co
country: co
silver_table_name: f_device_information_stage_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'ocurred_on', 'user_agent']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ApplicationDeviceUpdated': {'direct_attributes': ['application_id', 'ocurred_on', 'user_agent'], 'custom_attributes': {}}, 'ApplicationDeviceInformationUpdated': {'direct_attributes': ['application_id', 'ocurred_on', 'user_agent'], 'custom_attributes': {}}}
events_keys: ['ApplicationDeviceUpdated', 'ApplicationDeviceInformationUpdated']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
