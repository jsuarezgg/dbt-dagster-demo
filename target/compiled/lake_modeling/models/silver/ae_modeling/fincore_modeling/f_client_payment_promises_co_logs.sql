
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
        agent_info,client_id,NULL as client_payment_id,conditions,end_date,expected_amount,ocurred_on,payment_promise_id,resolution_call,stage,start_date,state,
    event_name,
    event_id
    FROM clientpaymentpromisegenerated_co
    UNION ALL
    SELECT 
        NULL as agent_info,client_id,client_payment_id,NULL as conditions,NULL as end_date,NULL as expected_amount,ocurred_on,payment_promise_id,NULL as resolution_call,stage,NULL as start_date,state,
    event_name,
    event_id
    FROM clientpaymentpromisefulfilled_co
    UNION ALL
    SELECT 
        NULL as agent_info,client_id,NULL as client_payment_id,NULL as conditions,NULL as end_date,NULL as expected_amount,ocurred_on,payment_promise_id,NULL as resolution_call,stage,NULL as start_date,state,
    event_name,
    event_id
    FROM clientpaymentpromiseunfulfilled_co
    UNION ALL
    SELECT 
        NULL as agent_info,client_id,NULL as client_payment_id,NULL as conditions,NULL as end_date,NULL as expected_amount,ocurred_on,payment_promise_id,NULL as resolution_call,stage,NULL as start_date,state,
    event_name,
    event_id
    FROM clientpaymentpromiseannulled_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    agent_info,client_id,client_payment_id,conditions,end_date,expected_amount,ocurred_on,payment_promise_id,resolution_call,stage,start_date,state,
    event_name,
    event_id
    FROM union_bronze 
    
)   



, final AS (
    SELECT 
        *,
        date(ocurred_on ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM union_all_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_client_payment_promises_co_logs
country: co
silver_table_name: f_client_payment_promises_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['agent_info', 'client_id', 'client_payment_id', 'conditions', 'end_date', 'event_id', 'expected_amount', 'ocurred_on', 'payment_promise_id', 'resolution_call', 'stage', 'start_date', 'state']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'clientpaymentpromisegenerated': {'direct_attributes': ['event_id', 'payment_promise_id', 'client_id', 'state', 'expected_amount', 'start_date', 'end_date', 'resolution_call', 'agent_info', 'conditions', 'ocurred_on', 'stage'], 'custom_attributes': {}}, 'clientpaymentpromisefulfilled': {'direct_attributes': ['event_id', 'payment_promise_id', 'client_id', 'state', 'client_payment_id', 'ocurred_on', 'stage'], 'custom_attributes': {}}, 'clientpaymentpromiseunfulfilled': {'direct_attributes': ['event_id', 'payment_promise_id', 'client_id', 'state', 'ocurred_on', 'stage'], 'custom_attributes': {}}, 'clientpaymentpromiseannulled': {'direct_attributes': ['event_id', 'payment_promise_id', 'client_id', 'state', 'ocurred_on', 'stage'], 'custom_attributes': {}}}
events_keys: ['clientpaymentpromisegenerated', 'clientpaymentpromisefulfilled', 'clientpaymentpromiseunfulfilled', 'clientpaymentpromiseannulled']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
