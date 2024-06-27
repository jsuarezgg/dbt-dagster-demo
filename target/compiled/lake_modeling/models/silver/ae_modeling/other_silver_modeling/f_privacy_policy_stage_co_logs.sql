
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
privacypolicyaccepted_co AS ( 
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


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,ocurred_on,privacy_policy_detail_json,
    event_name,
    event_id
    FROM privacypolicyaccepted_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,privacy_policy_detail_json,
    event_name,
    event_id
    FROM privacypolicyacceptedco_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,privacy_policy_detail_json,
    event_name,
    event_id
    FROM privacypolicyacceptedsantanderco_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,ocurred_on,privacy_policy_detail_json,
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
this: silver.f_privacy_policy_stage_co_logs
country: co
silver_table_name: f_privacy_policy_stage_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'ocurred_on', 'privacy_policy_detail_json']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'PrivacyPolicyAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'ocurred_on', 'application_id', 'client_id', 'privacy_policy_detail_json'], 'custom_attributes': {}}, 'PrivacyPolicyAcceptedCO': {'stage': 'privacy_policy_co', 'direct_attributes': ['event_id', 'ocurred_on', 'application_id', 'client_id', 'privacy_policy_detail_json'], 'custom_attributes': {}}, 'privacypolicyacceptedsantanderco': {'stage': 'privacy_policy_santander_co', 'direct_attributes': ['event_id', 'ocurred_on', 'application_id', 'client_id', 'privacy_policy_detail_json'], 'custom_attributes': {}}}
events_keys: ['PrivacyPolicyAccepted', 'PrivacyPolicyAcceptedCO', 'privacypolicyacceptedsantanderco']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
