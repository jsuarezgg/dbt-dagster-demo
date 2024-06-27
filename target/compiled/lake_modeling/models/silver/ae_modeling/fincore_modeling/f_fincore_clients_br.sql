
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientloansstatusupdatedv2_br AS ( 
    SELECT *
    FROM bronze.clientloansstatusupdatedv2_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        addicupo_last_update_reason,addicupo_remaining_balance,addicupo_source,addicupo_state,addicupo_total,client_id,delinquency_balance,event_mode,event_version,full_payment,installment_payment,min_payment,ocurred_on,ownership,payday,positive_balance,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientloansstatusupdatedv2_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    addicupo_last_update_reason,addicupo_remaining_balance,addicupo_source,addicupo_state,addicupo_total,client_id,delinquency_balance,event_mode,event_version,full_payment,installment_payment,min_payment,ocurred_on,ownership,payday,positive_balance,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    addicupo_last_update_reason,addicupo_remaining_balance,addicupo_source,addicupo_state,addicupo_total,client_id,delinquency_balance,event_mode,event_version,full_payment,installment_payment,min_payment,last_event_ocurred_on_processed as ocurred_on,ownership,payday,positive_balance,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_fincore_clients_br  
    WHERE 
    silver.f_fincore_clients_br.client_id IN (SELECT DISTINCT client_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    client_id,
    element_at(array_sort(array_agg(CASE WHEN addicupo_last_update_reason is not null then struct(ocurred_on, addicupo_last_update_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).addicupo_last_update_reason as addicupo_last_update_reason,
    element_at(array_sort(array_agg(CASE WHEN addicupo_remaining_balance is not null then struct(ocurred_on, addicupo_remaining_balance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).addicupo_remaining_balance as addicupo_remaining_balance,
    element_at(array_sort(array_agg(CASE WHEN addicupo_source is not null then struct(ocurred_on, addicupo_source) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).addicupo_source as addicupo_source,
    element_at(array_sort(array_agg(CASE WHEN addicupo_state is not null then struct(ocurred_on, addicupo_state) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).addicupo_state as addicupo_state,
    element_at(array_sort(array_agg(CASE WHEN addicupo_total is not null then struct(ocurred_on, addicupo_total) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).addicupo_total as addicupo_total,
    element_at(array_sort(array_agg(CASE WHEN delinquency_balance is not null then struct(ocurred_on, delinquency_balance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).delinquency_balance as delinquency_balance,
    element_at(array_sort(array_agg(CASE WHEN event_mode is not null then struct(ocurred_on, event_mode) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_mode as event_mode,
    element_at(array_sort(array_agg(CASE WHEN event_version is not null then struct(ocurred_on, event_version) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_version as event_version,
    element_at(array_sort(array_agg(CASE WHEN full_payment is not null then struct(ocurred_on, full_payment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).full_payment as full_payment,
    element_at(array_sort(array_agg(CASE WHEN installment_payment is not null then struct(ocurred_on, installment_payment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).installment_payment as installment_payment,
    element_at(array_sort(array_agg(CASE WHEN min_payment is not null then struct(ocurred_on, min_payment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).min_payment as min_payment,
    element_at(array_sort(array_agg(CASE WHEN ownership is not null then struct(ocurred_on, ownership) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ownership as ownership,
    element_at(array_sort(array_agg(CASE WHEN payday is not null then struct(ocurred_on, payday) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).payday as payday,
    element_at(array_sort(array_agg(CASE WHEN positive_balance is not null then struct(ocurred_on, positive_balance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).positive_balance as positive_balance,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    client_id
                       
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
this: silver.f_fincore_clients_br
country: br
silver_table_name: f_fincore_clients_br
table_pk_fields: ['client_id']
table_pk_amount: 1
fields_direct: ['addicupo_last_update_reason', 'addicupo_remaining_balance', 'addicupo_source', 'addicupo_state', 'addicupo_total', 'client_id', 'delinquency_balance', 'event_mode', 'event_version', 'full_payment', 'installment_payment', 'min_payment', 'ocurred_on', 'ownership', 'payday', 'positive_balance']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ClientLoansStatusUpdatedV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['addicupo_last_update_reason', 'addicupo_remaining_balance', 'addicupo_source', 'addicupo_state', 'addicupo_total', 'client_id', 'delinquency_balance', 'event_mode', 'event_version', 'full_payment', 'installment_payment', 'min_payment', 'ocurred_on', 'ownership', 'payday', 'positive_balance'], 'custom_attributes': {}}}
events_keys: ['ClientLoansStatusUpdatedV2']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
