
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
iovationobtained_unnested_by_result_rule_co AS ( 
    SELECT *
    FROM bronze.iovationobtained_unnested_by_result_rule_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,iovationobtained_v2_unnested_by_result_rule_co AS ( 
    SELECT *
    FROM bronze.iovationobtained_v2_unnested_by_result_rule_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,array_parent_path,client_id,custom_kyc_event_version,iovation_rule_result_reason,iovation_rule_result_score,iovation_rule_result_type,item_pseudo_idx,ocurred_on,surrogate_key,
    event_name,
    event_id
    FROM iovationobtained_unnested_by_result_rule_co
    UNION ALL
    SELECT 
        application_id,array_parent_path,client_id,custom_kyc_event_version,iovation_rule_result_reason,iovation_rule_result_score,iovation_rule_result_type,item_pseudo_idx,ocurred_on,surrogate_key,
    event_name,
    event_id
    FROM iovationobtained_v2_unnested_by_result_rule_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,array_parent_path,client_id,custom_kyc_event_version,iovation_rule_result_reason,iovation_rule_result_score,iovation_rule_result_type,item_pseudo_idx,ocurred_on,surrogate_key,
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
this: silver.f_kyc_iovation_v1v2_result_rules_co_logs
country: co
silver_table_name: f_kyc_iovation_v1v2_result_rules_co_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['application_id', 'array_parent_path', 'client_id', 'custom_kyc_event_version', 'event_id', 'iovation_rule_result_reason', 'iovation_rule_result_score', 'iovation_rule_result_type', 'item_pseudo_idx', 'ocurred_on', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'iovationobtained_unnested_by_result_rule': {'direct_attributes': ['surrogate_key', 'item_pseudo_idx', 'event_id', 'application_id', 'client_id', 'array_parent_path', 'custom_kyc_event_version', 'iovation_rule_result_reason', 'iovation_rule_result_score', 'iovation_rule_result_type', 'ocurred_on'], 'custom_attributes': {}}, 'iovationobtained_v2_unnested_by_result_rule': {'direct_attributes': ['surrogate_key', 'item_pseudo_idx', 'event_id', 'application_id', 'client_id', 'array_parent_path', 'custom_kyc_event_version', 'iovation_rule_result_reason', 'iovation_rule_result_score', 'iovation_rule_result_type', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['iovationobtained_unnested_by_result_rule', 'iovationobtained_v2_unnested_by_result_rule']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
