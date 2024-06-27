
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
        ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,loan_approved_amount,loan_cancellation_amount,NULL as loan_cancellation_annulment_reason,loan_cancellation_id,loan_cancellation_order_date,loan_cancellation_reason,loan_cancellation_type,loan_id,loan_origination_date,ocurred_on,store_user_name,user_id,user_role,
    event_name,
    event_id
    FROM loancancellationorderprocessedv2_br
    UNION ALL
    SELECT 
        NULL as ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,NULL as loan_approved_amount,NULL as loan_cancellation_amount,loan_cancellation_annulment_reason,loan_cancellation_id,NULL as loan_cancellation_order_date,NULL as loan_cancellation_reason,NULL as loan_cancellation_type,loan_id,NULL as loan_origination_date,ocurred_on,NULL as store_user_name,user_id,NULL as user_role,
    event_name,
    event_id
    FROM loancancellationorderannulledv2_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_loan_cancellation_status,loan_approved_amount,loan_cancellation_amount,loan_cancellation_annulment_reason,loan_cancellation_id,loan_cancellation_order_date,loan_cancellation_reason,loan_cancellation_type,loan_id,loan_origination_date,ocurred_on,store_user_name,user_id,user_role,
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
this: silver.f_loan_cancellations_v2_br_logs
country: br
silver_table_name: f_loan_cancellations_v2_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'client_id', 'custom_event_domain', 'custom_loan_cancellation_status', 'event_id', 'loan_approved_amount', 'loan_cancellation_amount', 'loan_cancellation_annulment_reason', 'loan_cancellation_id', 'loan_cancellation_order_date', 'loan_cancellation_reason', 'loan_cancellation_type', 'loan_id', 'loan_origination_date', 'ocurred_on', 'store_user_name', 'user_id', 'user_role']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'loancancellationorderprocessedv2': {'direct_attributes': ['event_id', 'loan_id', 'ally_slug', 'client_id', 'user_id', 'loan_cancellation_id', 'loan_cancellation_amount', 'loan_cancellation_type', 'loan_cancellation_reason', 'loan_cancellation_order_date', 'loan_approved_amount', 'loan_origination_date', 'user_role', 'store_user_name', 'custom_event_domain', 'custom_loan_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}, 'loancancellationorderannulledv2': {'direct_attributes': ['event_id', 'loan_id', 'client_id', 'user_id', 'loan_cancellation_annulment_reason', 'loan_cancellation_id', 'custom_event_domain', 'custom_loan_cancellation_status', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['loancancellationorderprocessedv2', 'loancancellationorderannulledv2']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
