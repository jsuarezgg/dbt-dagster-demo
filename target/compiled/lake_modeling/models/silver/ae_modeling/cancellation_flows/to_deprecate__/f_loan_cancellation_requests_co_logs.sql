
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
transactioncancellationrequested_co AS ( 
    SELECT *
    FROM bronze.transactioncancellationrequested_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,cancellation_amount,cancellation_date,cancellation_reason,cancellation_type,loan_id,loan_source,ocurred_on,source,
    event_name,
    event_id
    FROM transactioncancellationrequested_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,cancellation_amount,cancellation_date,cancellation_reason,cancellation_type,loan_id,loan_source,ocurred_on,source,
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
this: silver.f_loan_cancellation_requests_co_logs
country: co
silver_table_name: f_loan_cancellation_requests_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'cancellation_amount', 'cancellation_date', 'cancellation_reason', 'cancellation_type', 'event_id', 'loan_id', 'loan_source', 'ocurred_on', 'source']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'TransactionCancellationRequested': {'direct_attributes': ['ally_slug', 'cancellation_date', 'cancellation_reason', 'event_id', 'loan_id', 'loan_source', 'source', 'cancellation_type', 'cancellation_amount', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['TransactionCancellationRequested']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
