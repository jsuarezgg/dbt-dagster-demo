
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
        birth_city,birth_date,NULL as first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,
    event_name,
    event_id
    FROM prospectbureaupersonalinfoobtained_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,first_last_name,NULL as first_name,NULL as full_name,id_number,id_type,ocurred_on,client_id,
    event_name,
    event_id
    FROM privacypolicyaccepted_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,
    event_name,
    event_id
    FROM privacypolicyacceptedbr_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,
    event_name,
    event_id
    FROM checkoutloginacceptedbr_br
    UNION ALL
    SELECT 
        NULL as birth_city,birth_date,NULL as first_last_name,NULL as first_name,NULL as full_name,NULL as id_number,NULL as id_type,ocurred_on,client_id,
    event_name,
    event_id
    FROM basicidentityvalidatedbr_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    birth_city,birth_date,first_last_name,first_name,full_name,id_number,id_type,ocurred_on,client_id,
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
this: silver.d_prospect_personal_data_br_logs
country: br
silver_table_name: d_prospect_personal_data_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['birth_city', 'birth_date', 'event_id', 'first_last_name', 'first_name', 'full_name', 'id_number', 'id_type', 'ocurred_on', 'client_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ProspectBureauPersonalInfoObtained': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'first_name', 'full_name', 'birth_date', 'birth_city', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'first_last_name', 'birth_date', 'ocurred_on'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedBR': {'stage': 'privacy_policy_br', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'first_name', 'full_name', 'birth_date', 'first_last_name', 'ocurred_on'], 'custom_attributes': {}}, 'CheckoutLoginAcceptedBR': {'stage': 'expedited_checkout_login_br', 'direct_attributes': ['event_id', 'client_id', 'id_type', 'id_number', 'first_name', 'full_name', 'birth_date', 'first_last_name', 'ocurred_on'], 'custom_attributes': {}}, 'BasicIdentityValidatedBR': {'stage': 'basic_identity_br', 'direct_attributes': ['event_id', 'client_id', 'birth_date', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ProspectBureauPersonalInfoObtained', 'PrivacyPolicyAccepted', 'PrivacyPolicyAcceptedBR', 'CheckoutLoginAcceptedBR', 'BasicIdentityValidatedBR']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
