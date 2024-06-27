
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureaupersonalinfoobtained_br AS ( 
    SELECT *
    FROM bronze.prospectbureaupersonalinfoobtained_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,metadata_context_traceId,ocurred_on,personId_birth_city,personId_birth_date,personId_birth_state,personId_education,personId_firstName,personId_fullName,personId_gender,personId_idUpdateDate,personId_lastName,personId_maritalStatus,personId_mother_name_full,personId_number,personId_numberDependents,personId_partnerIdNumber,personId_status,personId_type,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectbureaupersonalinfoobtained_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,metadata_context_traceId,ocurred_on,personId_birth_city,personId_birth_date,personId_birth_state,personId_education,personId_firstName,personId_fullName,personId_gender,personId_idUpdateDate,personId_lastName,personId_maritalStatus,personId_mother_name_full,personId_number,personId_numberDependents,personId_partnerIdNumber,personId_status,personId_type,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,metadata_context_traceId,last_event_ocurred_on_processed as ocurred_on,personId_birth_city,personId_birth_date,personId_birth_state,personId_education,personId_firstName,personId_fullName,personId_gender,personId_idUpdateDate,personId_lastName,personId_maritalStatus,personId_mother_name_full,personId_number,personId_numberDependents,personId_partnerIdNumber,personId_status,personId_type,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_kyc_bureau_personal_info_br  
    WHERE 
    silver.f_kyc_bureau_personal_info_br.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN metadata_context_traceId is not null then struct(ocurred_on, metadata_context_traceId) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).metadata_context_traceId as metadata_context_traceId,
    element_at(array_sort(array_agg(CASE WHEN personId_birth_city is not null then struct(ocurred_on, personId_birth_city) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_birth_city as personId_birth_city,
    element_at(array_sort(array_agg(CASE WHEN personId_birth_date is not null then struct(ocurred_on, personId_birth_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_birth_date as personId_birth_date,
    element_at(array_sort(array_agg(CASE WHEN personId_birth_state is not null then struct(ocurred_on, personId_birth_state) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_birth_state as personId_birth_state,
    element_at(array_sort(array_agg(CASE WHEN personId_education is not null then struct(ocurred_on, personId_education) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_education as personId_education,
    element_at(array_sort(array_agg(CASE WHEN personId_firstName is not null then struct(ocurred_on, personId_firstName) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_firstName as personId_firstName,
    element_at(array_sort(array_agg(CASE WHEN personId_fullName is not null then struct(ocurred_on, personId_fullName) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_fullName as personId_fullName,
    element_at(array_sort(array_agg(CASE WHEN personId_gender is not null then struct(ocurred_on, personId_gender) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_gender as personId_gender,
    element_at(array_sort(array_agg(CASE WHEN personId_idUpdateDate is not null then struct(ocurred_on, personId_idUpdateDate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_idUpdateDate as personId_idUpdateDate,
    element_at(array_sort(array_agg(CASE WHEN personId_lastName is not null then struct(ocurred_on, personId_lastName) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_lastName as personId_lastName,
    element_at(array_sort(array_agg(CASE WHEN personId_maritalStatus is not null then struct(ocurred_on, personId_maritalStatus) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_maritalStatus as personId_maritalStatus,
    element_at(array_sort(array_agg(CASE WHEN personId_mother_name_full is not null then struct(ocurred_on, personId_mother_name_full) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_mother_name_full as personId_mother_name_full,
    element_at(array_sort(array_agg(CASE WHEN personId_number is not null then struct(ocurred_on, personId_number) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_number as personId_number,
    element_at(array_sort(array_agg(CASE WHEN personId_numberDependents is not null then struct(ocurred_on, personId_numberDependents) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_numberDependents as personId_numberDependents,
    element_at(array_sort(array_agg(CASE WHEN personId_partnerIdNumber is not null then struct(ocurred_on, personId_partnerIdNumber) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_partnerIdNumber as personId_partnerIdNumber,
    element_at(array_sort(array_agg(CASE WHEN personId_status is not null then struct(ocurred_on, personId_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_status as personId_status,
    element_at(array_sort(array_agg(CASE WHEN personId_type is not null then struct(ocurred_on, personId_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).personId_type as personId_type,
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
this: silver.f_kyc_bureau_personal_info_br
country: br
silver_table_name: f_kyc_bureau_personal_info_br
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'metadata_context_traceId', 'ocurred_on', 'personId_birth_city', 'personId_birth_date', 'personId_birth_state', 'personId_education', 'personId_firstName', 'personId_fullName', 'personId_gender', 'personId_idUpdateDate', 'personId_lastName', 'personId_maritalStatus', 'personId_mother_name_full', 'personId_number', 'personId_numberDependents', 'personId_partnerIdNumber', 'personId_status', 'personId_type']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureaupersonalinfoobtained': {'direct_attributes': ['application_id', 'client_id', 'personId_birth_city', 'personId_birth_date', 'personId_birth_state', 'personId_education', 'personId_firstName', 'personId_fullName', 'personId_gender', 'personId_idUpdateDate', 'personId_lastName', 'personId_maritalStatus', 'personId_mother_name_full', 'personId_number', 'personId_numberDependents', 'personId_partnerIdNumber', 'personId_status', 'personId_type', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureaupersonalinfoobtained']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
