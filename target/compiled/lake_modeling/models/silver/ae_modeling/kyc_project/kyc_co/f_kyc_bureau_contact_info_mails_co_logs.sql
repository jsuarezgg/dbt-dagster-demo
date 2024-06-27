
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureaucontactinfoobtained_unnested_by_mail_co AS ( 
    SELECT *
    FROM bronze.prospectbureaucontactinfoobtained_unnested_by_mail_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,array_parent_path,bureau_mail_address,bureau_mail_firstReport,bureau_mail_lastReport,bureau_mail_order,bureau_mail_timesReported,client_id,item_pseudo_idx,ocurred_on,surrogate_key,
    event_name,
    event_id
    FROM prospectbureaucontactinfoobtained_unnested_by_mail_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,array_parent_path,bureau_mail_address,bureau_mail_firstReport,bureau_mail_lastReport,bureau_mail_order,bureau_mail_timesReported,client_id,item_pseudo_idx,ocurred_on,surrogate_key,
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
this: silver.f_kyc_bureau_contact_info_mails_co_logs
country: co
silver_table_name: f_kyc_bureau_contact_info_mails_co_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['application_id', 'array_parent_path', 'bureau_mail_address', 'bureau_mail_firstReport', 'bureau_mail_lastReport', 'bureau_mail_order', 'bureau_mail_timesReported', 'client_id', 'event_id', 'item_pseudo_idx', 'ocurred_on', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureaucontactinfoobtained_unnested_by_mail': {'direct_attributes': ['surrogate_key', 'item_pseudo_idx', 'event_id', 'application_id', 'client_id', 'array_parent_path', 'bureau_mail_address', 'bureau_mail_firstReport', 'bureau_mail_lastReport', 'bureau_mail_order', 'bureau_mail_timesReported', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureaucontactinfoobtained_unnested_by_mail']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
