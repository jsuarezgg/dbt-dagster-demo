
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
idvthirdpartyapproved_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartycollected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartycollected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyrequested_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyrequested_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartystarted_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartystarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartymanualverificationrequired_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartymanualverificationrequired_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyphotorejected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyphotorejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyrejected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyrejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyskipped_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyskipped_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartyapproved_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartycollected_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartyrequested_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,NULL as idv_tp_unico_error_code,NULL as idv_tp_unico_process_id,NULL as idv_tp_unico_score,NULL as idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartystarted_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartymanualverificationrequired_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartyphotorejected_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,NULL as idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartyrejected_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,NULL as idvthirdpartyapproved_br_at,NULL as idvthirdpartycollected_br_at,NULL as idvthirdpartymanualverificationrequired_br_at,NULL as idvthirdpartyphotorejected_br_at,NULL as idvthirdpartyrejected_br_at,NULL as idvthirdpartyrequested_br_at,idvthirdpartyskipped_br_at,NULL as idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM idvthirdpartyskipped_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,idvthirdpartyapproved_br_at,idvthirdpartycollected_br_at,idvthirdpartymanualverificationrequired_br_at,idvthirdpartyphotorejected_br_at,idvthirdpartyrejected_br_at,idvthirdpartyrequested_br_at,idvthirdpartyskipped_br_at,idvthirdpartystarted_br_at,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,idvthirdpartyapproved_br_at,idvthirdpartycollected_br_at,idvthirdpartymanualverificationrequired_br_at,idvthirdpartyphotorejected_br_at,idvthirdpartyrejected_br_at,idvthirdpartyrequested_br_at,idvthirdpartyskipped_br_at,idvthirdpartystarted_br_at,last_event_ocurred_on_processed as ocurred_on,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_idv_third_party_br  
    WHERE 
    silver.f_idv_third_party_br.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN idv_tp_custom_last_status is not null then struct(ocurred_on, idv_tp_custom_last_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_tp_custom_last_status as idv_tp_custom_last_status,
    element_at(array_sort(array_agg(CASE WHEN idv_tp_provider is not null then struct(ocurred_on, idv_tp_provider) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_tp_provider as idv_tp_provider,
    element_at(array_sort(array_agg(CASE WHEN idv_tp_unico_error_code is not null then struct(ocurred_on, idv_tp_unico_error_code) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_tp_unico_error_code as idv_tp_unico_error_code,
    element_at(array_sort(array_agg(CASE WHEN idv_tp_unico_process_id is not null then struct(ocurred_on, idv_tp_unico_process_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_tp_unico_process_id as idv_tp_unico_process_id,
    element_at(array_sort(array_agg(CASE WHEN idv_tp_unico_score is not null then struct(ocurred_on, idv_tp_unico_score) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_tp_unico_score as idv_tp_unico_score,
    element_at(array_sort(array_agg(CASE WHEN idv_tp_unico_status is not null then struct(ocurred_on, idv_tp_unico_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_tp_unico_status as idv_tp_unico_status,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartyapproved_br_at is not null then struct(ocurred_on, idvthirdpartyapproved_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartyapproved_br_at as idvthirdpartyapproved_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartycollected_br_at is not null then struct(ocurred_on, idvthirdpartycollected_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartycollected_br_at as idvthirdpartycollected_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartymanualverificationrequired_br_at is not null then struct(ocurred_on, idvthirdpartymanualverificationrequired_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartymanualverificationrequired_br_at as idvthirdpartymanualverificationrequired_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartyphotorejected_br_at is not null then struct(ocurred_on, idvthirdpartyphotorejected_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartyphotorejected_br_at as idvthirdpartyphotorejected_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartyrejected_br_at is not null then struct(ocurred_on, idvthirdpartyrejected_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartyrejected_br_at as idvthirdpartyrejected_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartyrequested_br_at is not null then struct(ocurred_on, idvthirdpartyrequested_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartyrequested_br_at as idvthirdpartyrequested_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartyskipped_br_at is not null then struct(ocurred_on, idvthirdpartyskipped_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartyskipped_br_at as idvthirdpartyskipped_br_at,
    element_at(array_sort(array_agg(CASE WHEN idvthirdpartystarted_br_at is not null then struct(ocurred_on, idvthirdpartystarted_br_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idvthirdpartystarted_br_at as idvthirdpartystarted_br_at,
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
this: silver.f_idv_third_party_br
country: br
silver_table_name: f_idv_third_party_br
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_provider', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idvthirdpartyapproved_br_at', 'idvthirdpartycollected_br_at', 'idvthirdpartymanualverificationrequired_br_at', 'idvthirdpartyphotorejected_br_at', 'idvthirdpartyrejected_br_at', 'idvthirdpartyrequested_br_at', 'idvthirdpartyskipped_br_at', 'idvthirdpartystarted_br_at', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'idvthirdpartyapproved': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartyapproved_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartycollected': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartycollected_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyrequested': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartyrequested_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartystarted': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_provider', 'idvthirdpartystarted_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartymanualverificationrequired': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartymanualverificationrequired_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyphotorejected': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartyphotorejected_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyrejected': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartyrejected_br_at', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyskipped': {'direct_attributes': ['application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'idvthirdpartyskipped_br_at', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['idvthirdpartyapproved', 'idvthirdpartycollected', 'idvthirdpartyrequested', 'idvthirdpartystarted', 'idvthirdpartymanualverificationrequired', 'idvthirdpartyphotorejected', 'idvthirdpartyrejected', 'idvthirdpartyskipped']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
