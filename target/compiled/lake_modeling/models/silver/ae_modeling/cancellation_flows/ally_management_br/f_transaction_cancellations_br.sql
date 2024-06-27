
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
allytransactioncanceled_br AS ( 
    SELECT *
    FROM bronze.allytransactioncanceled_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,transactioncancellationrequested_br AS ( 
    SELECT *
    FROM bronze.transactioncancellationrequested_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,transactionrequestedtocancelbyid_br AS ( 
    SELECT *
    FROM bronze.transactionrequestedtocancelbyid_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,NULL as loan_source,ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allytransactioncanceled_br
    UNION ALL
    SELECT 
        ally_slug,NULL as client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,loan_source,ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,NULL as transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM transactioncancellationrequested_br
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as client_id,custom_event_domain,custom_transaction_cancellation_status,NULL as id_number,NULL as loan_id,NULL as loan_source,ocurred_on,NULL as store_user_name,transaction_cancellation_amount,transaction_cancellation_date,NULL as transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,NULL as transaction_subproduct,NULL as user_id,NULL as user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM transactionrequestedtocancelbyid_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,loan_source,ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,loan_source,last_event_ocurred_on_processed as ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_transaction_cancellations_br  
    WHERE 
    silver.f_transaction_cancellations_br.transaction_id IN (SELECT DISTINCT transaction_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    transaction_id,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN custom_event_domain is not null then struct(ocurred_on, custom_event_domain) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_event_domain as custom_event_domain,
    element_at(array_sort(array_agg(CASE WHEN custom_transaction_cancellation_status is not null then struct(ocurred_on, custom_transaction_cancellation_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_transaction_cancellation_status as custom_transaction_cancellation_status,
    element_at(array_sort(array_agg(CASE WHEN id_number is not null then struct(ocurred_on, id_number) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_number as id_number,
    element_at(array_sort(array_agg(CASE WHEN loan_id is not null then struct(ocurred_on, loan_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_id as loan_id,
    element_at(array_sort(array_agg(CASE WHEN loan_source is not null then struct(ocurred_on, loan_source) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_source as loan_source,
    element_at(array_sort(array_agg(CASE WHEN store_user_name is not null then struct(ocurred_on, store_user_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_user_name as store_user_name,
    element_at(array_sort(array_agg(CASE WHEN transaction_cancellation_amount is not null then struct(ocurred_on, transaction_cancellation_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_cancellation_amount as transaction_cancellation_amount,
    element_at(array_sort(array_agg(CASE WHEN transaction_cancellation_date is not null then struct(ocurred_on, transaction_cancellation_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_cancellation_date as transaction_cancellation_date,
    element_at(array_sort(array_agg(CASE WHEN transaction_cancellation_id is not null then struct(ocurred_on, transaction_cancellation_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_cancellation_id as transaction_cancellation_id,
    element_at(array_sort(array_agg(CASE WHEN transaction_cancellation_reason is not null then struct(ocurred_on, transaction_cancellation_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_cancellation_reason as transaction_cancellation_reason,
    element_at(array_sort(array_agg(CASE WHEN transaction_cancellation_source is not null then struct(ocurred_on, transaction_cancellation_source) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_cancellation_source as transaction_cancellation_source,
    element_at(array_sort(array_agg(CASE WHEN transaction_cancellation_type is not null then struct(ocurred_on, transaction_cancellation_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_cancellation_type as transaction_cancellation_type,
    element_at(array_sort(array_agg(CASE WHEN transaction_subproduct is not null then struct(ocurred_on, transaction_subproduct) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_subproduct as transaction_subproduct,
    element_at(array_sort(array_agg(CASE WHEN user_id is not null then struct(ocurred_on, user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_id as user_id,
    element_at(array_sort(array_agg(CASE WHEN user_role is not null then struct(ocurred_on, user_role) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_role as user_role,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    transaction_id
                       
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
this: silver.f_transaction_cancellations_br
country: br
silver_table_name: f_transaction_cancellations_br
table_pk_fields: ['transaction_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'client_id', 'custom_event_domain', 'custom_transaction_cancellation_status', 'id_number', 'loan_id', 'loan_source', 'ocurred_on', 'store_user_name', 'transaction_cancellation_amount', 'transaction_cancellation_date', 'transaction_cancellation_id', 'transaction_cancellation_reason', 'transaction_cancellation_source', 'transaction_cancellation_type', 'transaction_id', 'transaction_subproduct', 'user_id', 'user_role']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'allytransactioncanceled': {'direct_attributes': ['transaction_id', 'ally_slug', 'id_number', 'loan_id', 'client_id', 'user_id', 'user_role', 'store_user_name', 'transaction_cancellation_date', 'transaction_cancellation_reason', 'transaction_cancellation_amount', 'transaction_cancellation_id', 'transaction_cancellation_source', 'transaction_cancellation_type', 'transaction_subproduct', 'custom_event_domain', 'custom_transaction_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}, 'transactioncancellationrequested': {'direct_attributes': ['transaction_id', 'ally_slug', 'id_number', 'loan_id', 'loan_source', 'store_user_name', 'user_id', 'user_role', 'transaction_subproduct', 'transaction_cancellation_amount', 'transaction_cancellation_date', 'transaction_cancellation_reason', 'transaction_cancellation_source', 'transaction_cancellation_type', 'custom_event_domain', 'custom_transaction_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}, 'transactionrequestedtocancelbyid': {'direct_attributes': ['transaction_id', 'transaction_cancellation_amount', 'transaction_cancellation_date', 'transaction_cancellation_reason', 'transaction_cancellation_source', 'transaction_cancellation_type', 'custom_event_domain', 'custom_transaction_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['allytransactioncanceled', 'transactioncancellationrequested', 'transactionrequestedtocancelbyid']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
