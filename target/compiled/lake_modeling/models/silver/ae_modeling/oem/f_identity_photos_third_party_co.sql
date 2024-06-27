
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
identityphotosthirdpartyapproved_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscarded_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartymanualverificationrequired_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartymanualverificationrequired_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrejected_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrequested_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrequested_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartystarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartystarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,identityphotosthirdpartyapproved_co_at,NULL as identityphotosthirdpartydiscarded_co_at,NULL as identityphotosthirdpartydiscardedbyrisk_co_at,NULL as identityphotosthirdpartymanualverificationrequired_co_at,NULL as identityphotosthirdpartyrejected_co_at,NULL as identityphotosthirdpartyrequested_co_at,NULL as identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartyapproved_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,NULL as identityphotosthirdpartyapproved_co_at,identityphotosthirdpartydiscarded_co_at,NULL as identityphotosthirdpartydiscardedbyrisk_co_at,NULL as identityphotosthirdpartymanualverificationrequired_co_at,NULL as identityphotosthirdpartyrejected_co_at,NULL as identityphotosthirdpartyrequested_co_at,NULL as identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartydiscarded_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,NULL as identityphotosthirdpartyapproved_co_at,NULL as identityphotosthirdpartydiscarded_co_at,identityphotosthirdpartydiscardedbyrisk_co_at,NULL as identityphotosthirdpartymanualverificationrequired_co_at,NULL as identityphotosthirdpartyrejected_co_at,NULL as identityphotosthirdpartyrequested_co_at,NULL as identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartydiscardedbyrisk_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,NULL as identityphotosthirdpartyapproved_co_at,NULL as identityphotosthirdpartydiscarded_co_at,NULL as identityphotosthirdpartydiscardedbyrisk_co_at,identityphotosthirdpartymanualverificationrequired_co_at,NULL as identityphotosthirdpartyrejected_co_at,NULL as identityphotosthirdpartyrequested_co_at,NULL as identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartymanualverificationrequired_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,NULL as identityphotosthirdpartyapproved_co_at,NULL as identityphotosthirdpartydiscarded_co_at,NULL as identityphotosthirdpartydiscardedbyrisk_co_at,NULL as identityphotosthirdpartymanualverificationrequired_co_at,identityphotosthirdpartyrejected_co_at,NULL as identityphotosthirdpartyrequested_co_at,NULL as identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartyrejected_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,NULL as identityphotosthirdpartyapproved_co_at,NULL as identityphotosthirdpartydiscarded_co_at,NULL as identityphotosthirdpartydiscardedbyrisk_co_at,NULL as identityphotosthirdpartymanualverificationrequired_co_at,NULL as identityphotosthirdpartyrejected_co_at,identityphotosthirdpartyrequested_co_at,NULL as identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartyrequested_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,NULL as identityphotosthirdpartyapproved_co_at,NULL as identityphotosthirdpartydiscarded_co_at,NULL as identityphotosthirdpartydiscardedbyrisk_co_at,NULL as identityphotosthirdpartymanualverificationrequired_co_at,NULL as identityphotosthirdpartyrejected_co_at,NULL as identityphotosthirdpartyrequested_co_at,identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartystarted_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,identity_photos_tp_custom_last_status,identityphotosthirdpartyapproved_co_at,identityphotosthirdpartydiscarded_co_at,identityphotosthirdpartydiscardedbyrisk_co_at,identityphotosthirdpartymanualverificationrequired_co_at,identityphotosthirdpartyrejected_co_at,identityphotosthirdpartyrequested_co_at,identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,identity_photos_tp_custom_last_status,identityphotosthirdpartyapproved_co_at,identityphotosthirdpartydiscarded_co_at,identityphotosthirdpartydiscardedbyrisk_co_at,identityphotosthirdpartymanualverificationrequired_co_at,identityphotosthirdpartyrejected_co_at,identityphotosthirdpartyrequested_co_at,identityphotosthirdpartystarted_co_at,idv_provider,idv_third_party_attempts,last_event_ocurred_on_processed as ocurred_on,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_identity_photos_third_party_co  
    WHERE 
    silver.f_identity_photos_third_party_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN identity_photos_tp_custom_last_status is not null then struct(ocurred_on, identity_photos_tp_custom_last_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identity_photos_tp_custom_last_status as identity_photos_tp_custom_last_status,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartyapproved_co_at is not null then struct(ocurred_on, identityphotosthirdpartyapproved_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartyapproved_co_at as identityphotosthirdpartyapproved_co_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartydiscarded_co_at is not null then struct(ocurred_on, identityphotosthirdpartydiscarded_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartydiscarded_co_at as identityphotosthirdpartydiscarded_co_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartydiscardedbyrisk_co_at is not null then struct(ocurred_on, identityphotosthirdpartydiscardedbyrisk_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartydiscardedbyrisk_co_at as identityphotosthirdpartydiscardedbyrisk_co_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartymanualverificationrequired_co_at is not null then struct(ocurred_on, identityphotosthirdpartymanualverificationrequired_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartymanualverificationrequired_co_at as identityphotosthirdpartymanualverificationrequired_co_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartyrejected_co_at is not null then struct(ocurred_on, identityphotosthirdpartyrejected_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartyrejected_co_at as identityphotosthirdpartyrejected_co_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartyrequested_co_at is not null then struct(ocurred_on, identityphotosthirdpartyrequested_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartyrequested_co_at as identityphotosthirdpartyrequested_co_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosthirdpartystarted_co_at is not null then struct(ocurred_on, identityphotosthirdpartystarted_co_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosthirdpartystarted_co_at as identityphotosthirdpartystarted_co_at,
    element_at(array_sort(array_agg(CASE WHEN idv_provider is not null then struct(ocurred_on, idv_provider) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_provider as idv_provider,
    element_at(array_sort(array_agg(CASE WHEN idv_third_party_attempts is not null then struct(ocurred_on, idv_third_party_attempts) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_third_party_attempts as idv_third_party_attempts,
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
this: silver.f_identity_photos_third_party_co
country: co
silver_table_name: f_identity_photos_third_party_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'identityphotosthirdpartyapproved_co_at', 'identityphotosthirdpartydiscarded_co_at', 'identityphotosthirdpartydiscardedbyrisk_co_at', 'identityphotosthirdpartymanualverificationrequired_co_at', 'identityphotosthirdpartyrejected_co_at', 'identityphotosthirdpartyrequested_co_at', 'identityphotosthirdpartystarted_co_at', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'identityphotosthirdpartyapproved': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartyapproved_co_at', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscarded': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartydiscarded_co_at', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscardedbyrisk': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartydiscardedbyrisk_co_at', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartymanualverificationrequired': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartymanualverificationrequired_co_at', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrejected': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartyrejected_co_at', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrequested': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartyrequested_co_at', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartystarted': {'direct_attributes': ['application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'identityphotosthirdpartystarted_co_at', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['identityphotosthirdpartyapproved', 'identityphotosthirdpartydiscarded', 'identityphotosthirdpartydiscardedbyrisk', 'identityphotosthirdpartymanualverificationrequired', 'identityphotosthirdpartyrejected', 'identityphotosthirdpartyrequested', 'identityphotosthirdpartystarted']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
