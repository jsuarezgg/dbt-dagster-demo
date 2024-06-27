
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientpaymentannulledv2_co AS ( 
    SELECT *
    FROM bronze.clientpaymentannulledv2_co 
)
,clientpaymentreceivedv3_co AS ( 
    SELECT *
    FROM bronze.clientpaymentreceivedv3_co 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        annulment_reason,client_id,is_annulled,ocurred_on,NULL as paid_amount,NULL as payment_date,payment_id,NULL as payment_method,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpaymentannulledv2_co
    UNION ALL
    SELECT 
        annulment_reason,client_id,is_annulled,ocurred_on,paid_amount,payment_date,payment_id,payment_method,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpaymentreceivedv3_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    annulment_reason,client_id,is_annulled,ocurred_on,paid_amount,payment_date,payment_id,payment_method,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    payment_id,
    element_at(array_sort(array_agg(CASE WHEN annulment_reason is not null then struct(ocurred_on, annulment_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).annulment_reason as annulment_reason,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN is_annulled is not null then struct(ocurred_on, is_annulled) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).is_annulled as is_annulled,
    element_at(array_sort(array_agg(CASE WHEN paid_amount is not null then struct(ocurred_on, paid_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).paid_amount as paid_amount,
    element_at(array_sort(array_agg(CASE WHEN payment_date is not null then struct(ocurred_on, payment_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).payment_date as payment_date,
    element_at(array_sort(array_agg(CASE WHEN payment_method is not null then struct(ocurred_on, payment_method) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).payment_method as payment_method,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    payment_id
                       
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
this: silver.f_fincore_payments_co_v2
country: co
silver_table_name: f_fincore_payments_co_v2
table_pk_fields: ['payment_id']
table_pk_amount: 1
fields_direct: ['annulment_reason', 'client_id', 'is_annulled', 'ocurred_on', 'paid_amount', 'payment_date', 'payment_id', 'payment_method']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ClientPaymentAnnulledV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['payment_id', 'is_annulled', 'annulment_reason', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'ClientPaymentReceivedV3': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['payment_id', 'client_id', 'payment_method', 'paid_amount', 'payment_date', 'is_annulled', 'annulment_reason', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ClientPaymentAnnulledV2', 'ClientPaymentReceivedV3']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
