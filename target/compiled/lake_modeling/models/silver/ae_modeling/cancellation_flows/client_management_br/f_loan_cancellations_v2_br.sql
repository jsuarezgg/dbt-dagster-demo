
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
loancancellationorderprocessedv2_br AS ( 
    SELECT *
    FROM bronze.loancancellationorderprocessedv2_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loancancellationorderannulledv2_br AS ( 
    SELECT *
    FROM bronze.loancancellationorderannulledv2_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,loan_approved_amount,loan_cancellation_amount,NULL as loan_cancellation_annulment_reason,loan_cancellation_id,loan_cancellation_order_date,loan_cancellation_reason,loan_cancellation_type,loan_id,loan_origination_date,ocurred_on,store_user_name,user_id,user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loancancellationorderprocessedv2_br
    UNION ALL
    SELECT 
        NULL as ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,NULL as loan_approved_amount,NULL as loan_cancellation_amount,loan_cancellation_annulment_reason,loan_cancellation_id,NULL as loan_cancellation_order_date,NULL as loan_cancellation_reason,NULL as loan_cancellation_type,loan_id,NULL as loan_origination_date,ocurred_on,NULL as store_user_name,user_id,NULL as user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loancancellationorderannulledv2_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,loan_approved_amount,loan_cancellation_amount,loan_cancellation_annulment_reason,loan_cancellation_id,loan_cancellation_order_date,loan_cancellation_reason,loan_cancellation_type,loan_id,loan_origination_date,ocurred_on,store_user_name,user_id,user_role,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,loan_approved_amount,loan_cancellation_amount,loan_cancellation_annulment_reason,loan_cancellation_id,loan_cancellation_order_date,loan_cancellation_reason,loan_cancellation_type,loan_id,loan_origination_date,last_event_ocurred_on_processed as ocurred_on,store_user_name,user_id,user_role,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_loan_cancellations_v2_br  
    WHERE 
    silver.f_loan_cancellations_v2_br.loan_id IN (SELECT DISTINCT loan_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    loan_id,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN custom_event_domain is not null then struct(ocurred_on, custom_event_domain) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_event_domain as custom_event_domain,
    element_at(array_sort(array_agg(CASE WHEN custom_loan_cancellation_status is not null then struct(ocurred_on, custom_loan_cancellation_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_loan_cancellation_status as custom_loan_cancellation_status,
    element_at(array_sort(array_agg(CASE WHEN loan_approved_amount is not null then struct(ocurred_on, loan_approved_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_approved_amount as loan_approved_amount,
    element_at(array_sort(array_agg(CASE WHEN loan_cancellation_amount is not null then struct(ocurred_on, loan_cancellation_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_cancellation_amount as loan_cancellation_amount,
    element_at(array_sort(array_agg(CASE WHEN loan_cancellation_annulment_reason is not null then struct(ocurred_on, loan_cancellation_annulment_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_cancellation_annulment_reason as loan_cancellation_annulment_reason,
    element_at(array_sort(array_agg(CASE WHEN loan_cancellation_id is not null then struct(ocurred_on, loan_cancellation_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_cancellation_id as loan_cancellation_id,
    element_at(array_sort(array_agg(CASE WHEN loan_cancellation_order_date is not null then struct(ocurred_on, loan_cancellation_order_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_cancellation_order_date as loan_cancellation_order_date,
    element_at(array_sort(array_agg(CASE WHEN loan_cancellation_reason is not null then struct(ocurred_on, loan_cancellation_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_cancellation_reason as loan_cancellation_reason,
    element_at(array_sort(array_agg(CASE WHEN loan_cancellation_type is not null then struct(ocurred_on, loan_cancellation_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_cancellation_type as loan_cancellation_type,
    element_at(array_sort(array_agg(CASE WHEN loan_origination_date is not null then struct(ocurred_on, loan_origination_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_origination_date as loan_origination_date,
    element_at(array_sort(array_agg(CASE WHEN store_user_name is not null then struct(ocurred_on, store_user_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_user_name as store_user_name,
    element_at(array_sort(array_agg(CASE WHEN user_id is not null then struct(ocurred_on, user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_id as user_id,
    element_at(array_sort(array_agg(CASE WHEN user_role is not null then struct(ocurred_on, user_role) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_role as user_role,
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
is_incremental: True
this: silver.f_loan_cancellations_v2_br
country: br
silver_table_name: f_loan_cancellations_v2_br
table_pk_fields: ['loan_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'client_id', 'custom_event_domain', 'custom_loan_cancellation_status', 'loan_approved_amount', 'loan_cancellation_amount', 'loan_cancellation_annulment_reason', 'loan_cancellation_id', 'loan_cancellation_order_date', 'loan_cancellation_reason', 'loan_cancellation_type', 'loan_id', 'loan_origination_date', 'ocurred_on', 'store_user_name', 'user_id', 'user_role']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'loancancellationorderprocessedv2': {'direct_attributes': ['loan_id', 'ally_slug', 'client_id', 'user_id', 'loan_cancellation_id', 'loan_cancellation_amount', 'loan_cancellation_type', 'loan_cancellation_reason', 'loan_cancellation_order_date', 'loan_approved_amount', 'loan_origination_date', 'user_role', 'store_user_name', 'custom_event_domain', 'custom_loan_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}, 'loancancellationorderannulledv2': {'direct_attributes': ['loan_id', 'client_id', 'user_id', 'loan_cancellation_annulment_reason', 'loan_cancellation_id', 'custom_event_domain', 'custom_loan_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['loancancellationorderprocessedv2', 'loancancellationorderannulledv2']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
