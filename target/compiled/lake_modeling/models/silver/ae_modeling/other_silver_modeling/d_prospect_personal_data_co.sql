
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureaupersonalinfoobtained_co AS ( 
    SELECT *
    FROM bronze.prospectbureaupersonalinfoobtained_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyaccepted_co AS ( 
    SELECT *
    FROM bronze.privacypolicyaccepted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedco_co AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedsantanderco_co AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,basicidentityvalidatedco_co AS ( 
    SELECT *
    FROM bronze.basicidentityvalidatedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectbureaupersonalinfoobtained_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,NULL as full_name,id_number,id_type,last_name,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyaccepted_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyacceptedco_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyacceptedsantanderco_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,NULL as full_name,NULL as id_number,NULL as id_type,NULL as last_name,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM basicidentityvalidatedco_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,last_event_ocurred_on_processed as ocurred_on,client_id,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.d_prospect_personal_data_co  
    WHERE 
    silver.d_prospect_personal_data_co.client_id IN (SELECT DISTINCT client_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    client_id,
    element_at(array_sort(array_agg(CASE WHEN document_expedition_city is not null then struct(ocurred_on, document_expedition_city) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).document_expedition_city as document_expedition_city,
    element_at(array_sort(array_agg(CASE WHEN document_expedition_date is not null then struct(ocurred_on, document_expedition_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).document_expedition_date as document_expedition_date,
    element_at(array_sort(array_agg(CASE WHEN full_name is not null then struct(ocurred_on, full_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).full_name as full_name,
    element_at(array_sort(array_agg(CASE WHEN id_number is not null then struct(ocurred_on, id_number) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_number as id_number,
    element_at(array_sort(array_agg(CASE WHEN id_type is not null then struct(ocurred_on, id_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_type as id_type,
    element_at(array_sort(array_agg(CASE WHEN last_name is not null then struct(ocurred_on, last_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_name as last_name,
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
this: silver.d_prospect_personal_data_co
country: co
silver_table_name: d_prospect_personal_data_co
table_pk_fields: ['client_id']
table_pk_amount: 1
fields_direct: ['document_expedition_city', 'document_expedition_date', 'full_name', 'id_number', 'id_type', 'last_name', 'ocurred_on', 'client_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ProspectBureauPersonalInfoObtained': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'last_name', 'full_name', 'document_expedition_city', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'last_name', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedCO': {'stage': 'privacy_policy_co', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'last_name', 'full_name', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedSantanderCO': {'stage': 'privacy_policy_santander_co', 'direct_attributes': ['client_id', 'id_type', 'id_number', 'last_name', 'full_name', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'BasicIdentityValidatedCO': {'stage': 'basic_identity_co', 'direct_attributes': ['client_id', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ProspectBureauPersonalInfoObtained', 'PrivacyPolicyAccepted', 'PrivacyPolicyAcceptedCO', 'PrivacyPolicyAcceptedSantanderCO', 'BasicIdentityValidatedCO']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
