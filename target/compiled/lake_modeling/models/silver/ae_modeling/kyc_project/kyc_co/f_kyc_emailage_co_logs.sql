
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
emailageobtained_co AS ( 
    SELECT *
    FROM bronze.emailageobtained_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,emailAge_advice_id,emailAge_advice_value,emailAge_birthDate,emailAge_company,emailAge_country,emailAge_domain_age,emailAge_domain_creationDays,emailAge_domain_exits,emailAge_domain_name,emailAge_domain_riskLevel,emailAge_domain_riskLevelId,emailAge_eName,emailAge_email_age,emailAge_email_creationDays,emailAge_email_exists,emailAge_email_value,emailAge_firstSeenDays,emailAge_firstVerificationDate,emailAge_fraudType,emailAge_hits_total,emailAge_hits_unique,emailAge_imageUrl,emailAge_lastVerificationDate,emailAge_lastflaggedon,emailAge_location,emailAge_reason_id,emailAge_reason_value,emailAge_riskBand_id,emailAge_riskBand_value,emailAge_score,emailAge_socialMediaFriends,emailAge_sourceIndustry,emailAge_status_id,emailAge_status_value,emailAge_title,metadata_context_traceId,ocurred_on,
    event_name,
    event_id
    FROM emailageobtained_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,emailAge_advice_id,emailAge_advice_value,emailAge_birthDate,emailAge_company,emailAge_country,emailAge_domain_age,emailAge_domain_creationDays,emailAge_domain_exits,emailAge_domain_name,emailAge_domain_riskLevel,emailAge_domain_riskLevelId,emailAge_eName,emailAge_email_age,emailAge_email_creationDays,emailAge_email_exists,emailAge_email_value,emailAge_firstSeenDays,emailAge_firstVerificationDate,emailAge_fraudType,emailAge_hits_total,emailAge_hits_unique,emailAge_imageUrl,emailAge_lastVerificationDate,emailAge_lastflaggedon,emailAge_location,emailAge_reason_id,emailAge_reason_value,emailAge_riskBand_id,emailAge_riskBand_value,emailAge_score,emailAge_socialMediaFriends,emailAge_sourceIndustry,emailAge_status_id,emailAge_status_value,emailAge_title,metadata_context_traceId,ocurred_on,
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
this: silver.f_kyc_emailage_co_logs
country: co
silver_table_name: f_kyc_emailage_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'emailAge_advice_id', 'emailAge_advice_value', 'emailAge_birthDate', 'emailAge_company', 'emailAge_country', 'emailAge_domain_age', 'emailAge_domain_creationDays', 'emailAge_domain_exits', 'emailAge_domain_name', 'emailAge_domain_riskLevel', 'emailAge_domain_riskLevelId', 'emailAge_eName', 'emailAge_email_age', 'emailAge_email_creationDays', 'emailAge_email_exists', 'emailAge_email_value', 'emailAge_firstSeenDays', 'emailAge_firstVerificationDate', 'emailAge_fraudType', 'emailAge_hits_total', 'emailAge_hits_unique', 'emailAge_imageUrl', 'emailAge_lastVerificationDate', 'emailAge_lastflaggedon', 'emailAge_location', 'emailAge_reason_id', 'emailAge_reason_value', 'emailAge_riskBand_id', 'emailAge_riskBand_value', 'emailAge_score', 'emailAge_socialMediaFriends', 'emailAge_sourceIndustry', 'emailAge_status_id', 'emailAge_status_value', 'emailAge_title', 'event_id', 'metadata_context_traceId', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'emailageobtained': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'emailAge_advice_id', 'emailAge_advice_value', 'emailAge_birthDate', 'emailAge_company', 'emailAge_country', 'emailAge_domain_age', 'emailAge_domain_creationDays', 'emailAge_domain_exits', 'emailAge_domain_name', 'emailAge_domain_riskLevel', 'emailAge_domain_riskLevelId', 'emailAge_eName', 'emailAge_email_age', 'emailAge_email_creationDays', 'emailAge_email_exists', 'emailAge_email_value', 'emailAge_firstSeenDays', 'emailAge_firstVerificationDate', 'emailAge_fraudType', 'emailAge_hits_total', 'emailAge_hits_unique', 'emailAge_imageUrl', 'emailAge_lastVerificationDate', 'emailAge_lastflaggedon', 'emailAge_location', 'emailAge_reason_id', 'emailAge_reason_value', 'emailAge_riskBand_id', 'emailAge_riskBand_value', 'emailAge_score', 'emailAge_socialMediaFriends', 'emailAge_sourceIndustry', 'emailAge_status_id', 'emailAge_status_value', 'emailAge_title', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['emailageobtained']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
