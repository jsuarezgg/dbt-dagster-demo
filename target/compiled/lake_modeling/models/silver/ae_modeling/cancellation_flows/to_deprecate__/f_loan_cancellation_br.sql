
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
loancancellationorderprocessedv2_br AS ( 
    SELECT *
    FROM bronze.loancancellationorderprocessedv2_br 
)
,loancancellationorderannulledv2_br AS ( 
    SELECT *
    FROM bronze.loancancellationorderannulledv2_br 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        NULL as annulment_reason,cancellation_date,cancellation_id,cancellation_reason,client_id,loan_id,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loancancellationorderprocessedv2_br
    UNION ALL
    SELECT 
        annulment_reason,NULL as cancellation_date,cancellation_id,NULL as cancellation_reason,client_id,loan_id,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loancancellationorderannulledv2_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    annulment_reason,cancellation_date,cancellation_id,cancellation_reason,client_id,loan_id,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    loan_id,
    element_at(array_sort(array_agg(CASE WHEN annulment_reason is not null then struct(ocurred_on, annulment_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).annulment_reason as annulment_reason,
    element_at(array_sort(array_agg(CASE WHEN cancellation_date is not null then struct(ocurred_on, cancellation_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).cancellation_date as cancellation_date,
    element_at(array_sort(array_agg(CASE WHEN cancellation_id is not null then struct(ocurred_on, cancellation_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).cancellation_id as cancellation_id,
    element_at(array_sort(array_agg(CASE WHEN cancellation_reason is not null then struct(ocurred_on, cancellation_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).cancellation_reason as cancellation_reason,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    loan_id
                       
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
is_incremental: False
this: silver.f_loan_cancellation_br
country: br
silver_table_name: f_loan_cancellation_br
table_pk_fields: ['loan_id']
table_pk_amount: 1
fields_direct: ['annulment_reason', 'cancellation_date', 'cancellation_id', 'cancellation_reason', 'client_id', 'loan_id', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'LoanCancellationOrderProcessedV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_id', 'client_id', 'cancellation_date', 'cancellation_reason', 'cancellation_id', 'ocurred_on'], 'custom_attributes': {}}, 'LoanCancellationOrderAnnulledV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_id', 'client_id', 'annulment_reason', 'cancellation_id', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['LoanCancellationOrderProcessedV2', 'LoanCancellationOrderAnnulledV2']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
