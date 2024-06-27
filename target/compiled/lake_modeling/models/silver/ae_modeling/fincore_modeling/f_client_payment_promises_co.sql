
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientpaymentpromisegenerated_co AS ( 
    SELECT *
    FROM bronze.clientpaymentpromisegenerated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentpromisefulfilled_co AS ( 
    SELECT *
    FROM bronze.clientpaymentpromisefulfilled_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentpromiseunfulfilled_co AS ( 
    SELECT *
    FROM bronze.clientpaymentpromiseunfulfilled_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentpromiseannulled_co AS ( 
    SELECT *
    FROM bronze.clientpaymentpromiseannulled_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        agent_info,client_id,NULL as client_payment_id,conditions,end_date,expected_amount,ocurred_on,payment_promise_id,resolution_call,stage,start_date,state,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpaymentpromisegenerated_co
    UNION ALL
    SELECT 
        NULL as agent_info,client_id,client_payment_id,NULL as conditions,NULL as end_date,NULL as expected_amount,ocurred_on,payment_promise_id,NULL as resolution_call,stage,NULL as start_date,state,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpaymentpromisefulfilled_co
    UNION ALL
    SELECT 
        NULL as agent_info,client_id,NULL as client_payment_id,NULL as conditions,NULL as end_date,NULL as expected_amount,ocurred_on,payment_promise_id,NULL as resolution_call,stage,NULL as start_date,state,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpaymentpromiseunfulfilled_co
    UNION ALL
    SELECT 
        NULL as agent_info,client_id,NULL as client_payment_id,NULL as conditions,NULL as end_date,NULL as expected_amount,ocurred_on,payment_promise_id,NULL as resolution_call,stage,NULL as start_date,state,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpaymentpromiseannulled_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    agent_info,client_id,client_payment_id,conditions,end_date,expected_amount,ocurred_on,payment_promise_id,resolution_call,stage,start_date,state,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    agent_info,client_id,client_payment_id,conditions,end_date,expected_amount,last_event_ocurred_on_processed as ocurred_on,payment_promise_id,resolution_call,stage,start_date,state,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_client_payment_promises_co  
    WHERE 
    silver.f_client_payment_promises_co.payment_promise_id IN (SELECT DISTINCT payment_promise_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    payment_promise_id,
    element_at(array_sort(array_agg(CASE WHEN agent_info is not null then struct(ocurred_on, agent_info) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).agent_info as agent_info,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN client_payment_id is not null then struct(ocurred_on, client_payment_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_payment_id as client_payment_id,
    element_at(array_sort(array_agg(CASE WHEN conditions is not null then struct(ocurred_on, conditions) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).conditions as conditions,
    element_at(array_sort(array_agg(CASE WHEN end_date is not null then struct(ocurred_on, end_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).end_date as end_date,
    element_at(array_sort(array_agg(CASE WHEN expected_amount is not null then struct(ocurred_on, expected_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).expected_amount as expected_amount,
    element_at(array_sort(array_agg(CASE WHEN resolution_call is not null then struct(ocurred_on, resolution_call) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).resolution_call as resolution_call,
    element_at(array_sort(array_agg(CASE WHEN stage is not null then struct(ocurred_on, stage) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).stage as stage,
    element_at(array_sort(array_agg(CASE WHEN start_date is not null then struct(ocurred_on, start_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).start_date as start_date,
    element_at(array_sort(array_agg(CASE WHEN state is not null then struct(ocurred_on, state) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).state as state,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    payment_promise_id
                       
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
this: silver.f_client_payment_promises_co
country: co
silver_table_name: f_client_payment_promises_co
table_pk_fields: ['payment_promise_id']
table_pk_amount: 1
fields_direct: ['agent_info', 'client_id', 'client_payment_id', 'conditions', 'end_date', 'expected_amount', 'ocurred_on', 'payment_promise_id', 'resolution_call', 'stage', 'start_date', 'state']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'clientpaymentpromisegenerated': {'direct_attributes': ['payment_promise_id', 'client_id', 'state', 'expected_amount', 'start_date', 'end_date', 'resolution_call', 'agent_info', 'conditions', 'ocurred_on', 'stage'], 'custom_attributes': {}}, 'clientpaymentpromisefulfilled': {'direct_attributes': ['payment_promise_id', 'client_id', 'state', 'client_payment_id', 'ocurred_on', 'stage'], 'custom_attributes': {}}, 'clientpaymentpromiseunfulfilled': {'direct_attributes': ['payment_promise_id', 'client_id', 'state', 'ocurred_on', 'stage'], 'custom_attributes': {}}, 'clientpaymentpromiseannulled': {'direct_attributes': ['payment_promise_id', 'client_id', 'state', 'ocurred_on', 'stage'], 'custom_attributes': {}}}
events_keys: ['clientpaymentpromisegenerated', 'clientpaymentpromisefulfilled', 'clientpaymentpromiseunfulfilled', 'clientpaymentpromiseannulled']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
