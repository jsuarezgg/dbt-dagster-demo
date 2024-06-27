
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureauincomevalidatorobtained_co AS ( 
    SELECT *
    FROM bronze.prospectbureauincomevalidatorobtained_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,income_healthEntity,income_pensionFundName,metadata_context_traceId,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectbureauincomevalidatorobtained_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,income_healthEntity,income_pensionFundName,metadata_context_traceId,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,income_healthEntity,income_pensionFundName,metadata_context_traceId,last_event_ocurred_on_processed as ocurred_on,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_kyc_bureau_income_validator_co  
    WHERE 
    silver.f_kyc_bureau_income_validator_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN income_healthEntity is not null then struct(ocurred_on, income_healthEntity) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_healthEntity as income_healthEntity,
    element_at(array_sort(array_agg(CASE WHEN income_pensionFundName is not null then struct(ocurred_on, income_pensionFundName) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_pensionFundName as income_pensionFundName,
    element_at(array_sort(array_agg(CASE WHEN metadata_context_traceId is not null then struct(ocurred_on, metadata_context_traceId) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).metadata_context_traceId as metadata_context_traceId,
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
this: silver.f_kyc_bureau_income_validator_co
country: co
silver_table_name: f_kyc_bureau_income_validator_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'income_healthEntity', 'income_pensionFundName', 'metadata_context_traceId', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureauincomevalidatorobtained': {'direct_attributes': ['application_id', 'client_id', 'income_healthEntity', 'income_pensionFundName', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureauincomevalidatorobtained']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
