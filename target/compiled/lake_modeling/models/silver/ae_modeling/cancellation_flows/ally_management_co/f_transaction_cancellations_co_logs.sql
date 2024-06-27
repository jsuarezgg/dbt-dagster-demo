
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
allytransactioncanceled_co AS ( 
    SELECT *
    FROM bronze.allytransactioncanceled_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,transactioncancellationrequested_co AS ( 
    SELECT *
    FROM bronze.transactioncancellationrequested_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,transactionrequestedtocancelbyid_co AS ( 
    SELECT *
    FROM bronze.transactionrequestedtocancelbyid_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,NULL as loan_source,ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,
    event_name,
    event_id
    FROM allytransactioncanceled_co
    UNION ALL
    SELECT 
        ally_slug,NULL as client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,loan_source,ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,NULL as transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,
    event_name,
    event_id
    FROM transactioncancellationrequested_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as client_id,custom_event_domain,custom_transaction_cancellation_status,NULL as id_number,NULL as loan_id,NULL as loan_source,ocurred_on,NULL as store_user_name,transaction_cancellation_amount,transaction_cancellation_date,NULL as transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,NULL as transaction_subproduct,NULL as user_id,NULL as user_role,
    event_name,
    event_id
    FROM transactionrequestedtocancelbyid_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_transaction_cancellation_status,id_number,loan_id,loan_source,ocurred_on,store_user_name,transaction_cancellation_amount,transaction_cancellation_date,transaction_cancellation_id,transaction_cancellation_reason,transaction_cancellation_source,transaction_cancellation_type,transaction_id,transaction_subproduct,user_id,user_role,
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
this: silver.f_transaction_cancellations_co_logs
country: co
silver_table_name: f_transaction_cancellations_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'client_id', 'custom_event_domain', 'custom_transaction_cancellation_status', 'event_id', 'id_number', 'loan_id', 'loan_source', 'ocurred_on', 'store_user_name', 'transaction_cancellation_amount', 'transaction_cancellation_date', 'transaction_cancellation_id', 'transaction_cancellation_reason', 'transaction_cancellation_source', 'transaction_cancellation_type', 'transaction_id', 'transaction_subproduct', 'user_id', 'user_role']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'allytransactioncanceled': {'direct_attributes': ['event_id', 'transaction_id', 'ally_slug', 'id_number', 'loan_id', 'client_id', 'user_id', 'user_role', 'store_user_name', 'transaction_cancellation_date', 'transaction_cancellation_reason', 'transaction_cancellation_amount', 'transaction_cancellation_id', 'transaction_cancellation_source', 'transaction_cancellation_type', 'transaction_subproduct', 'custom_event_domain', 'custom_transaction_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}, 'transactioncancellationrequested': {'direct_attributes': ['event_id', 'transaction_id', 'ally_slug', 'id_number', 'loan_id', 'loan_source', 'store_user_name', 'user_id', 'user_role', 'transaction_subproduct', 'transaction_cancellation_amount', 'transaction_cancellation_date', 'transaction_cancellation_reason', 'transaction_cancellation_source', 'transaction_cancellation_type', 'custom_event_domain', 'custom_transaction_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}, 'transactionrequestedtocancelbyid': {'direct_attributes': ['event_id', 'transaction_id', 'transaction_cancellation_amount', 'transaction_cancellation_date', 'transaction_cancellation_reason', 'transaction_cancellation_source', 'transaction_cancellation_type', 'custom_event_domain', 'custom_transaction_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['allytransactioncanceled', 'transactioncancellationrequested', 'transactionrequestedtocancelbyid']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
