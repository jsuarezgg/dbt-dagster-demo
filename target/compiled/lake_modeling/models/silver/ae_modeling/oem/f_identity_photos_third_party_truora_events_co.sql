
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
identityphotosthirdpartyapproved_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyapproved_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscarded_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscarded_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrejected_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrejected_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrequested_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrequested_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartyapproved_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartydiscarded_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartyrejected_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosthirdpartyrequested_unnested_by_truora_event_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,last_event_ocurred_on_processed as ocurred_on,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_identity_photos_third_party_truora_events_co  
    WHERE 
    silver.f_identity_photos_third_party_truora_events_co.truora_event_validation_id IN (SELECT DISTINCT truora_event_validation_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    truora_event_validation_id,
    element_at(array_sort(array_agg(CASE WHEN application_id is not null then struct(ocurred_on, application_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_id as application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN truora_event_confidence_score is not null then struct(ocurred_on, truora_event_confidence_score) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_confidence_score as truora_event_confidence_score,
    element_at(array_sort(array_agg(CASE WHEN truora_event_declined_reason is not null then struct(ocurred_on, truora_event_declined_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_declined_reason as truora_event_declined_reason,
    element_at(array_sort(array_agg(CASE WHEN truora_event_failure_status is not null then struct(ocurred_on, truora_event_failure_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_failure_status as truora_event_failure_status,
    element_at(array_sort(array_agg(CASE WHEN truora_event_threshold is not null then struct(ocurred_on, truora_event_threshold) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_threshold as truora_event_threshold,
    element_at(array_sort(array_agg(CASE WHEN truora_event_timestamp is not null then struct(ocurred_on, truora_event_timestamp) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_timestamp as truora_event_timestamp,
    element_at(array_sort(array_agg(CASE WHEN truora_event_type is not null then struct(ocurred_on, truora_event_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_type as truora_event_type,
    element_at(array_sort(array_agg(CASE WHEN truora_event_validation_status is not null then struct(ocurred_on, truora_event_validation_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).truora_event_validation_status as truora_event_validation_status,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    truora_event_validation_id
                       
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
this: silver.f_identity_photos_third_party_truora_events_co
country: co
silver_table_name: f_identity_photos_third_party_truora_events_co
table_pk_fields: ['truora_event_validation_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'ocurred_on', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'truora_event_type', 'truora_event_validation_id', 'truora_event_validation_status']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'identityphotosthirdpartyapproved_unnested_by_truora_event': {'direct_attributes': ['truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscarded_unnested_by_truora_event': {'direct_attributes': ['truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event': {'direct_attributes': ['truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event': {'direct_attributes': ['truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrejected_unnested_by_truora_event': {'direct_attributes': ['truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrequested_unnested_by_truora_event': {'direct_attributes': ['truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['identityphotosthirdpartyapproved_unnested_by_truora_event', 'identityphotosthirdpartydiscarded_unnested_by_truora_event', 'identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event', 'identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event', 'identityphotosthirdpartyrejected_unnested_by_truora_event', 'identityphotosthirdpartyrequested_unnested_by_truora_event']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
