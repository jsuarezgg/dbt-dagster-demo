
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
        document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,
    event_name,
    event_id
    FROM prospectbureaupersonalinfoobtained_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,NULL as full_name,id_number,id_type,last_name,ocurred_on,client_id,
    event_name,
    event_id
    FROM privacypolicyaccepted_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,
    event_name,
    event_id
    FROM privacypolicyacceptedco_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,
    event_name,
    event_id
    FROM privacypolicyacceptedsantanderco_co
    UNION ALL
    SELECT 
        NULL as document_expedition_city,document_expedition_date,NULL as full_name,NULL as id_number,NULL as id_type,NULL as last_name,ocurred_on,client_id,
    event_name,
    event_id
    FROM basicidentityvalidatedco_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    document_expedition_city,document_expedition_date,full_name,id_number,id_type,last_name,ocurred_on,client_id,
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
this: silver.d_prospect_personal_data_co_logs
country: co
silver_table_name: d_prospect_personal_data_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['document_expedition_city', 'document_expedition_date', 'event_id', 'full_name', 'id_number', 'id_type', 'last_name', 'ocurred_on', 'client_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ProspectBureauPersonalInfoObtained': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'last_name', 'full_name', 'document_expedition_city', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'last_name', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedCO': {'stage': 'privacy_policy_co', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'last_name', 'full_name', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedSantanderCO': {'stage': 'privacy_policy_santander_co', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'last_name', 'full_name', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}, 'BasicIdentityValidatedCO': {'stage': 'basic_identity_co', 'direct_attributes': ['event_id', 'client_id', 'document_expedition_date', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ProspectBureauPersonalInfoObtained', 'PrivacyPolicyAccepted', 'PrivacyPolicyAcceptedCO', 'PrivacyPolicyAcceptedSantanderCO', 'BasicIdentityValidatedCO']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
