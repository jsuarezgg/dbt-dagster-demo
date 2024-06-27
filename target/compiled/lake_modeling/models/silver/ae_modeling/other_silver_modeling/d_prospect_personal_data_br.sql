
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureaupersonalinfoobtained_br AS ( 
    SELECT *
    FROM bronze.prospectbureaupersonalinfoobtained_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyaccepted_br AS ( 
    SELECT *
    FROM bronze.privacypolicyaccepted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedbr_br AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptedbr_br AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,basicidentityvalidatedbr_br AS ( 
    SELECT *
    FROM bronze.basicidentityvalidatedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        birth_city,birth_date,NULL as first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectbureaupersonalinfoobtained_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,first_last_name,NULL as first_name,NULL as full_name,id_number,id_type,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyaccepted_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyacceptedbr_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM checkoutloginacceptedbr_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,NULL as first_last_name,NULL as first_name,NULL as full_name,NULL as id_number,NULL as id_type,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM basicidentityvalidatedbr_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,last_event_ocurred_on_processed as ocurred_on,client_id,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.d_prospect_personal_data_br  
    WHERE 
    silver.d_prospect_personal_data_br.client_id IN (SELECT DISTINCT client_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    client_id,
    element_at(array_sort(array_agg(CASE WHEN birth_city is not null then struct(ocurred_on, birth_city) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).birth_city as birth_city,
    element_at(array_sort(array_agg(CASE WHEN birth_date is not null then struct(ocurred_on, birth_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).birth_date as birth_date,
    element_at(array_sort(array_agg(CASE WHEN first_last_name is not null then struct(ocurred_on, first_last_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_last_name as first_last_name,
    element_at(array_sort(array_agg(CASE WHEN first_name is not null then struct(ocurred_on, first_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_name as first_name,
    element_at(array_sort(array_agg(CASE WHEN full_name is not null then struct(ocurred_on, full_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).full_name as full_name,
    element_at(array_sort(array_agg(CASE WHEN id_number is not null then struct(ocurred_on, id_number) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_number as id_number,
    element_at(array_sort(array_agg(CASE WHEN id_type is not null then struct(ocurred_on, id_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_type as id_type,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    client_id
                       
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
this: silver.d_prospect_personal_data_br
country: br
silver_table_name: d_prospect_personal_data_br
table_pk_fields: ['client_id']
table_pk_amount: 1
fields_direct: ['birth_city', 'birth_date', 'first_last_name', 'first_name', 'full_name', 'id_number', 'id_type', 'ocurred_on', 'client_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ProspectBureauPersonalInfoObtained': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'first_name', 'full_name', 'birth_date', 'birth_city', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'first_last_name', 'birth_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedBR': {'stage': 'privacy_policy_br', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'first_name', 'full_name', 'birth_date', 'first_last_name', 'ocurred_on'], 'custom_attributes': {}}, 'CheckoutLoginAcceptedBR': {'stage': 'expedited_checkout_login_br', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'first_name', 'full_name', 'birth_date', 'first_last_name', 'ocurred_on'], 'custom_attributes': {}}, 'BasicIdentityValidatedBR': {'stage': 'basic_identity_br', 'direct_attributes': ['client_id', 'birth_date', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ProspectBureauPersonalInfoObtained', 'PrivacyPolicyAccepted', 'PrivacyPolicyAcceptedBR', 'CheckoutLoginAcceptedBR', 'BasicIdentityValidatedBR']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
