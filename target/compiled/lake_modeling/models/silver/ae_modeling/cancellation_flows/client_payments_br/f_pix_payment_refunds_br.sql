
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
pixpaymentrefunded_br AS ( 
    SELECT *
    FROM bronze.pixpaymentrefunded_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,client_id,custom_event_domain,custom_payment_refund_status,ocurred_on,pix_payment_amount,pix_payment_number,pix_payment_refund_occurred_on,pix_payment_refund_reason,user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM pixpaymentrefunded_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_payment_refund_status,ocurred_on,pix_payment_amount,pix_payment_number,pix_payment_refund_occurred_on,pix_payment_refund_reason,user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_payment_refund_status,last_event_ocurred_on_processed as ocurred_on,pix_payment_amount,pix_payment_number,pix_payment_refund_occurred_on,pix_payment_refund_reason,user_id,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_pix_payment_refunds_br  
    WHERE 
    silver.f_pix_payment_refunds_br.pix_payment_number IN (SELECT DISTINCT pix_payment_number FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    pix_payment_number,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN custom_event_domain is not null then struct(ocurred_on, custom_event_domain) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_event_domain as custom_event_domain,
    element_at(array_sort(array_agg(CASE WHEN custom_payment_refund_status is not null then struct(ocurred_on, custom_payment_refund_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_payment_refund_status as custom_payment_refund_status,
    element_at(array_sort(array_agg(CASE WHEN pix_payment_amount is not null then struct(ocurred_on, pix_payment_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).pix_payment_amount as pix_payment_amount,
    element_at(array_sort(array_agg(CASE WHEN pix_payment_refund_occurred_on is not null then struct(ocurred_on, pix_payment_refund_occurred_on) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).pix_payment_refund_occurred_on as pix_payment_refund_occurred_on,
    element_at(array_sort(array_agg(CASE WHEN pix_payment_refund_reason is not null then struct(ocurred_on, pix_payment_refund_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).pix_payment_refund_reason as pix_payment_refund_reason,
    element_at(array_sort(array_agg(CASE WHEN user_id is not null then struct(ocurred_on, user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_id as user_id,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    pix_payment_number
                       
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
this: silver.f_pix_payment_refunds_br
country: br
silver_table_name: f_pix_payment_refunds_br
table_pk_fields: ['pix_payment_number']
table_pk_amount: 1
fields_direct: ['ally_slug', 'client_id', 'custom_event_domain', 'custom_payment_refund_status', 'ocurred_on', 'pix_payment_amount', 'pix_payment_number', 'pix_payment_refund_occurred_on', 'pix_payment_refund_reason', 'user_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'pixpaymentrefunded': {'direct_attributes': ['pix_payment_number', 'ally_slug', 'client_id', 'user_id', 'pix_payment_amount', 'pix_payment_refund_occurred_on', 'pix_payment_refund_reason', 'custom_event_domain', 'custom_payment_refund_status', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['pixpaymentrefunded']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
