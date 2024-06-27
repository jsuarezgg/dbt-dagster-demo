
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
loancancellationorderprocessedv2_co AS ( 
    SELECT *
    FROM bronze.loancancellationorderprocessedv2_co 
)
,loancancellationorderannulledv2_co AS ( 
    SELECT *
    FROM bronze.loancancellationorderannulledv2_co 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        NULL as annulment_reason,cancellation_date,cancellation_id,cancellation_reason,client_id,loan_id,ocurred_on,
    event_name,
    event_id
    FROM loancancellationorderprocessedv2_co
    UNION ALL
    SELECT 
        annulment_reason,NULL as cancellation_date,cancellation_id,NULL as cancellation_reason,client_id,loan_id,ocurred_on,
    event_name,
    event_id
    FROM loancancellationorderannulledv2_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    annulment_reason,cancellation_date,cancellation_id,cancellation_reason,client_id,loan_id,ocurred_on,
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
is_incremental: False
this: silver.f_loan_cancellation_co_logs
country: co
silver_table_name: f_loan_cancellation_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['annulment_reason', 'cancellation_date', 'cancellation_id', 'cancellation_reason', 'client_id', 'event_id', 'loan_id', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'LoanCancellationOrderProcessedV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'loan_id', 'client_id', 'cancellation_reason', 'ocurred_on', 'cancellation_date', 'cancellation_id'], 'custom_attributes': {}}, 'LoanCancellationOrderAnnulledV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'loan_id', 'client_id', 'annulment_reason', 'ocurred_on', 'cancellation_id'], 'custom_attributes': {}}}
events_keys: ['LoanCancellationOrderProcessedV2', 'LoanCancellationOrderAnnulledV2']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
