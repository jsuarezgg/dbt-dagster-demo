
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
loansoldtotrust_co AS ( 
    SELECT *
    FROM bronze.loansoldtotrust_co 
)
,loanreturnedfromtrust_co AS ( 
    SELECT *
    FROM bronze.loanreturnedfromtrust_co 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ocurred_on,client_id,loan_id,loan_ownership,sold_on,sold_amount,custom_is_sold,custom_is_returned,custom_loan_ownership_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loansoldtotrust_co
    UNION ALL
    SELECT 
        ocurred_on,client_id,loan_id,loan_ownership,NULL as sold_on,NULL as sold_amount,custom_is_sold,custom_is_returned,custom_loan_ownership_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanreturnedfromtrust_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ocurred_on,client_id,loan_id,loan_ownership,sold_on,sold_amount,custom_is_sold,custom_is_returned,custom_loan_ownership_status,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    loan_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN loan_ownership is not null then struct(ocurred_on, loan_ownership) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_ownership as loan_ownership,
    element_at(array_sort(array_agg(CASE WHEN sold_on is not null then struct(ocurred_on, sold_on) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).sold_on as sold_on,
    element_at(array_sort(array_agg(CASE WHEN sold_amount is not null then struct(ocurred_on, sold_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).sold_amount as sold_amount,
    element_at(array_sort(array_agg(CASE WHEN custom_is_sold is not null then struct(ocurred_on, custom_is_sold) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_sold as custom_is_sold,
    element_at(array_sort(array_agg(CASE WHEN custom_is_returned is not null then struct(ocurred_on, custom_is_returned) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_returned as custom_is_returned,
    element_at(array_sort(array_agg(CASE WHEN custom_loan_ownership_status is not null then struct(ocurred_on, custom_loan_ownership_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_loan_ownership_status as custom_loan_ownership_status,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    loan_id
                       
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
this: silver.f_loan_ownership_co
country: co
silver_table_name: f_loan_ownership_co
table_pk_fields: ['loan_id']
table_pk_amount: 1
fields_direct: ['event_id', 'ocurred_on', 'client_id', 'loan_id', 'loan_ownership', 'sold_on', 'sold_amount', 'custom_is_sold', 'custom_is_returned', 'custom_loan_ownership_status']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'loansoldtotrust': {'direct_attributes': ['event_id', 'ocurred_on', 'client_id', 'loan_id', 'loan_ownership', 'sold_on', 'sold_amount', 'custom_is_sold', 'custom_is_returned', 'custom_loan_ownership_status'], 'custom_attributes': {}}, 'loanreturnedfromtrust': {'direct_attributes': ['event_id', 'ocurred_on', 'client_id', 'loan_id', 'loan_ownership', 'custom_is_sold', 'custom_is_returned', 'custom_loan_ownership_status'], 'custom_attributes': {}}}
events_keys: ['loansoldtotrust', 'loanreturnedfromtrust']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
