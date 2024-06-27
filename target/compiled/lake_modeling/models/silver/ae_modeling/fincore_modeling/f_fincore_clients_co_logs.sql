
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientloansstatusupdatedv2_co AS ( 
    SELECT *
    FROM bronze.clientloansstatusupdatedv2_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        addicupo_last_update_reason,addicupo_remaining_balance,addicupo_source,addicupo_state,addicupo_total,client_id,delinquency_balance,event_mode,event_version,full_payment,installment_payment,min_payment,ocurred_on,ownership,payday,positive_balance,
    event_name,
    event_id
    FROM clientloansstatusupdatedv2_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    addicupo_last_update_reason,addicupo_remaining_balance,addicupo_source,addicupo_state,addicupo_total,client_id,delinquency_balance,event_mode,event_version,full_payment,installment_payment,min_payment,ocurred_on,ownership,payday,positive_balance,
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
this: silver.f_fincore_clients_co_logs
country: co
silver_table_name: f_fincore_clients_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['addicupo_last_update_reason', 'addicupo_remaining_balance', 'addicupo_source', 'addicupo_state', 'addicupo_total', 'client_id', 'delinquency_balance', 'event_id', 'event_mode', 'event_version', 'full_payment', 'installment_payment', 'min_payment', 'ocurred_on', 'ownership', 'payday', 'positive_balance']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ClientLoansStatusUpdatedV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'addicupo_last_update_reason', 'addicupo_remaining_balance', 'addicupo_source', 'addicupo_state', 'addicupo_total', 'client_id', 'delinquency_balance', 'event_mode', 'event_version', 'full_payment', 'installment_payment', 'min_payment', 'ocurred_on', 'ownership', 'payday', 'positive_balance'], 'custom_attributes': {}}}
events_keys: ['ClientLoansStatusUpdatedV2']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
