
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
wompitransactioncreatedv2_co AS ( 
    SELECT *
    FROM bronze.wompitransactioncreatedv2_co 
)
,wompitransactionupdatedv2_co AS ( 
    SELECT *
    FROM bronze.wompitransactionupdatedv2_co 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        channel,client_id,ocurred_on,product,transaction_amount,NULL as transaction_business_agreement_code,transaction_currency,transaction_financial_institution_code,transaction_origin,transaction_payment_description,NULL as transaction_payment_intention_identifier,transaction_reference,transaction_status,transaction_status_reason,transaction_type,
    event_name,
    event_id
    FROM wompitransactioncreatedv2_co
    UNION ALL
    SELECT 
        NULL as channel,client_id,ocurred_on,NULL as product,transaction_amount,transaction_business_agreement_code,transaction_currency,transaction_financial_institution_code,transaction_origin,transaction_payment_description,transaction_payment_intention_identifier,transaction_reference,transaction_status,transaction_status_reason,transaction_type,
    event_name,
    event_id
    FROM wompitransactionupdatedv2_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    channel,client_id,ocurred_on,product,transaction_amount,transaction_business_agreement_code,transaction_currency,transaction_financial_institution_code,transaction_origin,transaction_payment_description,transaction_payment_intention_identifier,transaction_reference,transaction_status,transaction_status_reason,transaction_type,
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
this: silver.f_wompi_transactions_co_logs
country: co
silver_table_name: f_wompi_transactions_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['channel', 'client_id', 'event_id', 'ocurred_on', 'product', 'transaction_amount', 'transaction_business_agreement_code', 'transaction_currency', 'transaction_financial_institution_code', 'transaction_origin', 'transaction_payment_description', 'transaction_payment_intention_identifier', 'transaction_reference', 'transaction_status', 'transaction_status_reason', 'transaction_type']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'wompitransactioncreatedv2': {'direct_attributes': ['event_id', 'ocurred_on', 'client_id', 'channel', 'product', 'transaction_amount', 'transaction_currency', 'transaction_origin', 'transaction_financial_institution_code', 'transaction_payment_description', 'transaction_type', 'transaction_reference', 'transaction_status', 'transaction_status_reason'], 'custom_attributes': {}}, 'wompitransactionupdatedv2': {'direct_attributes': ['event_id', 'ocurred_on', 'client_id', 'transaction_amount', 'transaction_currency', 'transaction_origin', 'transaction_business_agreement_code', 'transaction_payment_intention_identifier', 'transaction_financial_institution_code', 'transaction_payment_description', 'transaction_type', 'transaction_reference', 'transaction_status', 'transaction_status_reason'], 'custom_attributes': {}}}
events_keys: ['wompitransactioncreatedv2', 'wompitransactionupdatedv2']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
