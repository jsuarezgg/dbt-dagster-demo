
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureaupersonalinfoobtained_unnested_by_person_id_contingency_br AS ( 
    SELECT *
    FROM bronze.prospectbureaupersonalinfoobtained_unnested_by_person_id_contingency_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,array_parent_path,bureau_person_id_contingency_idNumber,bureau_person_id_contingency_idType,bureau_person_id_contingency_idTypeDescription,bureau_person_id_contingency_reportDate,bureau_person_id_contingency_reportReason,client_id,item_pseudo_idx,ocurred_on,surrogate_key,
    event_name,
    event_id
    FROM prospectbureaupersonalinfoobtained_unnested_by_person_id_contingency_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,array_parent_path,bureau_person_id_contingency_idNumber,bureau_person_id_contingency_idType,bureau_person_id_contingency_idTypeDescription,bureau_person_id_contingency_reportDate,bureau_person_id_contingency_reportReason,client_id,item_pseudo_idx,ocurred_on,surrogate_key,
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
this: silver.f_kyc_bureau_personal_info_person_id_contingencies_br_logs
country: br
silver_table_name: f_kyc_bureau_personal_info_person_id_contingencies_br_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['application_id', 'array_parent_path', 'bureau_person_id_contingency_idNumber', 'bureau_person_id_contingency_idType', 'bureau_person_id_contingency_idTypeDescription', 'bureau_person_id_contingency_reportDate', 'bureau_person_id_contingency_reportReason', 'client_id', 'event_id', 'item_pseudo_idx', 'ocurred_on', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureaupersonalinfoobtained_unnested_by_person_id_contingency': {'direct_attributes': ['surrogate_key', 'item_pseudo_idx', 'event_id', 'application_id', 'client_id', 'array_parent_path', 'bureau_person_id_contingency_idNumber', 'bureau_person_id_contingency_idType', 'bureau_person_id_contingency_idTypeDescription', 'bureau_person_id_contingency_reportDate', 'bureau_person_id_contingency_reportReason', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureaupersonalinfoobtained_unnested_by_person_id_contingency']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
