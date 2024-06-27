
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
        channel,client_id,ocurred_on,product,transaction_amount,NULL as transaction_business_agreement_code,transaction_currency,transaction_financial_institution_code,transaction_origin,transaction_payment_description,NULL as transaction_payment_intention_identifier,transaction_reference,transaction_status,transaction_status_reason,transaction_type,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM wompitransactioncreatedv2_co
    UNION ALL
    SELECT 
        NULL as channel,client_id,ocurred_on,NULL as product,transaction_amount,transaction_business_agreement_code,transaction_currency,transaction_financial_institution_code,transaction_origin,transaction_payment_description,transaction_payment_intention_identifier,transaction_reference,transaction_status,transaction_status_reason,transaction_type,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM wompitransactionupdatedv2_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    channel,client_id,ocurred_on,product,transaction_amount,transaction_business_agreement_code,transaction_currency,transaction_financial_institution_code,transaction_origin,transaction_payment_description,transaction_payment_intention_identifier,transaction_reference,transaction_status,transaction_status_reason,transaction_type,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    transaction_reference,
    element_at(array_sort(array_agg(CASE WHEN channel is not null then struct(ocurred_on, channel) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).channel as channel,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN product is not null then struct(ocurred_on, product) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).product as product,
    element_at(array_sort(array_agg(CASE WHEN transaction_amount is not null then struct(ocurred_on, transaction_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_amount as transaction_amount,
    element_at(array_sort(array_agg(CASE WHEN transaction_business_agreement_code is not null then struct(ocurred_on, transaction_business_agreement_code) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_business_agreement_code as transaction_business_agreement_code,
    element_at(array_sort(array_agg(CASE WHEN transaction_currency is not null then struct(ocurred_on, transaction_currency) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_currency as transaction_currency,
    element_at(array_sort(array_agg(CASE WHEN transaction_financial_institution_code is not null then struct(ocurred_on, transaction_financial_institution_code) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_financial_institution_code as transaction_financial_institution_code,
    element_at(array_sort(array_agg(CASE WHEN transaction_origin is not null then struct(ocurred_on, transaction_origin) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_origin as transaction_origin,
    element_at(array_sort(array_agg(CASE WHEN transaction_payment_description is not null then struct(ocurred_on, transaction_payment_description) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_payment_description as transaction_payment_description,
    element_at(array_sort(array_agg(CASE WHEN transaction_payment_intention_identifier is not null then struct(ocurred_on, transaction_payment_intention_identifier) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_payment_intention_identifier as transaction_payment_intention_identifier,
    element_at(array_sort(array_agg(CASE WHEN transaction_status is not null then struct(ocurred_on, transaction_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_status as transaction_status,
    element_at(array_sort(array_agg(CASE WHEN transaction_status_reason is not null then struct(ocurred_on, transaction_status_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_status_reason as transaction_status_reason,
    element_at(array_sort(array_agg(CASE WHEN transaction_type is not null then struct(ocurred_on, transaction_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).transaction_type as transaction_type,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    transaction_reference
                       
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
is_incremental: False
this: silver.f_wompi_transactions_co
country: co
silver_table_name: f_wompi_transactions_co
table_pk_fields: ['transaction_reference']
table_pk_amount: 1
fields_direct: ['channel', 'client_id', 'ocurred_on', 'product', 'transaction_amount', 'transaction_business_agreement_code', 'transaction_currency', 'transaction_financial_institution_code', 'transaction_origin', 'transaction_payment_description', 'transaction_payment_intention_identifier', 'transaction_reference', 'transaction_status', 'transaction_status_reason', 'transaction_type']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'wompitransactioncreatedv2': {'direct_attributes': ['ocurred_on', 'client_id', 'channel', 'product', 'transaction_amount', 'transaction_currency', 'transaction_origin', 'transaction_financial_institution_code', 'transaction_payment_description', 'transaction_type', 'transaction_reference', 'transaction_status', 'transaction_status_reason'], 'custom_attributes': {}}, 'wompitransactionupdatedv2': {'direct_attributes': ['ocurred_on', 'client_id', 'transaction_amount', 'transaction_currency', 'transaction_origin', 'transaction_business_agreement_code', 'transaction_payment_intention_identifier', 'transaction_financial_institution_code', 'transaction_payment_description', 'transaction_type', 'transaction_reference', 'transaction_status', 'transaction_status_reason'], 'custom_attributes': {}}}
events_keys: ['wompitransactioncreatedv2', 'wompitransactionupdatedv2']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
