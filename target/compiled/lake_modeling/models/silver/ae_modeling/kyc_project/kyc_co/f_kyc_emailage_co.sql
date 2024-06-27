
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
        application_id,client_id,emailAge_advice_id,emailAge_advice_value,emailAge_birthDate,emailAge_company,emailAge_country,emailAge_domain_age,emailAge_domain_creationDays,emailAge_domain_exits,emailAge_domain_name,emailAge_domain_riskLevel,emailAge_domain_riskLevelId,emailAge_eName,emailAge_email_age,emailAge_email_creationDays,emailAge_email_exists,emailAge_email_value,emailAge_firstSeenDays,emailAge_firstVerificationDate,emailAge_fraudType,emailAge_hits_total,emailAge_hits_unique,emailAge_imageUrl,emailAge_lastVerificationDate,emailAge_lastflaggedon,emailAge_location,emailAge_reason_id,emailAge_reason_value,emailAge_riskBand_id,emailAge_riskBand_value,emailAge_score,emailAge_socialMediaFriends,emailAge_sourceIndustry,emailAge_status_id,emailAge_status_value,emailAge_title,metadata_context_traceId,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM emailageobtained_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,emailAge_advice_id,emailAge_advice_value,emailAge_birthDate,emailAge_company,emailAge_country,emailAge_domain_age,emailAge_domain_creationDays,emailAge_domain_exits,emailAge_domain_name,emailAge_domain_riskLevel,emailAge_domain_riskLevelId,emailAge_eName,emailAge_email_age,emailAge_email_creationDays,emailAge_email_exists,emailAge_email_value,emailAge_firstSeenDays,emailAge_firstVerificationDate,emailAge_fraudType,emailAge_hits_total,emailAge_hits_unique,emailAge_imageUrl,emailAge_lastVerificationDate,emailAge_lastflaggedon,emailAge_location,emailAge_reason_id,emailAge_reason_value,emailAge_riskBand_id,emailAge_riskBand_value,emailAge_score,emailAge_socialMediaFriends,emailAge_sourceIndustry,emailAge_status_id,emailAge_status_value,emailAge_title,metadata_context_traceId,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,emailAge_advice_id,emailAge_advice_value,emailAge_birthDate,emailAge_company,emailAge_country,emailAge_domain_age,emailAge_domain_creationDays,emailAge_domain_exits,emailAge_domain_name,emailAge_domain_riskLevel,emailAge_domain_riskLevelId,emailAge_eName,emailAge_email_age,emailAge_email_creationDays,emailAge_email_exists,emailAge_email_value,emailAge_firstSeenDays,emailAge_firstVerificationDate,emailAge_fraudType,emailAge_hits_total,emailAge_hits_unique,emailAge_imageUrl,emailAge_lastVerificationDate,emailAge_lastflaggedon,emailAge_location,emailAge_reason_id,emailAge_reason_value,emailAge_riskBand_id,emailAge_riskBand_value,emailAge_score,emailAge_socialMediaFriends,emailAge_sourceIndustry,emailAge_status_id,emailAge_status_value,emailAge_title,metadata_context_traceId,last_event_ocurred_on_processed as ocurred_on,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_kyc_emailage_co  
    WHERE 
    silver.f_kyc_emailage_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN emailAge_advice_id is not null then struct(ocurred_on, emailAge_advice_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_advice_id as emailAge_advice_id,
    element_at(array_sort(array_agg(CASE WHEN emailAge_advice_value is not null then struct(ocurred_on, emailAge_advice_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_advice_value as emailAge_advice_value,
    element_at(array_sort(array_agg(CASE WHEN emailAge_birthDate is not null then struct(ocurred_on, emailAge_birthDate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_birthDate as emailAge_birthDate,
    element_at(array_sort(array_agg(CASE WHEN emailAge_company is not null then struct(ocurred_on, emailAge_company) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_company as emailAge_company,
    element_at(array_sort(array_agg(CASE WHEN emailAge_country is not null then struct(ocurred_on, emailAge_country) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_country as emailAge_country,
    element_at(array_sort(array_agg(CASE WHEN emailAge_domain_age is not null then struct(ocurred_on, emailAge_domain_age) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_domain_age as emailAge_domain_age,
    element_at(array_sort(array_agg(CASE WHEN emailAge_domain_creationDays is not null then struct(ocurred_on, emailAge_domain_creationDays) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_domain_creationDays as emailAge_domain_creationDays,
    element_at(array_sort(array_agg(CASE WHEN emailAge_domain_exits is not null then struct(ocurred_on, emailAge_domain_exits) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_domain_exits as emailAge_domain_exits,
    element_at(array_sort(array_agg(CASE WHEN emailAge_domain_name is not null then struct(ocurred_on, emailAge_domain_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_domain_name as emailAge_domain_name,
    element_at(array_sort(array_agg(CASE WHEN emailAge_domain_riskLevel is not null then struct(ocurred_on, emailAge_domain_riskLevel) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_domain_riskLevel as emailAge_domain_riskLevel,
    element_at(array_sort(array_agg(CASE WHEN emailAge_domain_riskLevelId is not null then struct(ocurred_on, emailAge_domain_riskLevelId) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_domain_riskLevelId as emailAge_domain_riskLevelId,
    element_at(array_sort(array_agg(CASE WHEN emailAge_eName is not null then struct(ocurred_on, emailAge_eName) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_eName as emailAge_eName,
    element_at(array_sort(array_agg(CASE WHEN emailAge_email_age is not null then struct(ocurred_on, emailAge_email_age) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_email_age as emailAge_email_age,
    element_at(array_sort(array_agg(CASE WHEN emailAge_email_creationDays is not null then struct(ocurred_on, emailAge_email_creationDays) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_email_creationDays as emailAge_email_creationDays,
    element_at(array_sort(array_agg(CASE WHEN emailAge_email_exists is not null then struct(ocurred_on, emailAge_email_exists) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_email_exists as emailAge_email_exists,
    element_at(array_sort(array_agg(CASE WHEN emailAge_email_value is not null then struct(ocurred_on, emailAge_email_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_email_value as emailAge_email_value,
    element_at(array_sort(array_agg(CASE WHEN emailAge_firstSeenDays is not null then struct(ocurred_on, emailAge_firstSeenDays) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_firstSeenDays as emailAge_firstSeenDays,
    element_at(array_sort(array_agg(CASE WHEN emailAge_firstVerificationDate is not null then struct(ocurred_on, emailAge_firstVerificationDate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_firstVerificationDate as emailAge_firstVerificationDate,
    element_at(array_sort(array_agg(CASE WHEN emailAge_fraudType is not null then struct(ocurred_on, emailAge_fraudType) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_fraudType as emailAge_fraudType,
    element_at(array_sort(array_agg(CASE WHEN emailAge_hits_total is not null then struct(ocurred_on, emailAge_hits_total) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_hits_total as emailAge_hits_total,
    element_at(array_sort(array_agg(CASE WHEN emailAge_hits_unique is not null then struct(ocurred_on, emailAge_hits_unique) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_hits_unique as emailAge_hits_unique,
    element_at(array_sort(array_agg(CASE WHEN emailAge_imageUrl is not null then struct(ocurred_on, emailAge_imageUrl) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_imageUrl as emailAge_imageUrl,
    element_at(array_sort(array_agg(CASE WHEN emailAge_lastVerificationDate is not null then struct(ocurred_on, emailAge_lastVerificationDate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_lastVerificationDate as emailAge_lastVerificationDate,
    element_at(array_sort(array_agg(CASE WHEN emailAge_lastflaggedon is not null then struct(ocurred_on, emailAge_lastflaggedon) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_lastflaggedon as emailAge_lastflaggedon,
    element_at(array_sort(array_agg(CASE WHEN emailAge_location is not null then struct(ocurred_on, emailAge_location) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_location as emailAge_location,
    element_at(array_sort(array_agg(CASE WHEN emailAge_reason_id is not null then struct(ocurred_on, emailAge_reason_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_reason_id as emailAge_reason_id,
    element_at(array_sort(array_agg(CASE WHEN emailAge_reason_value is not null then struct(ocurred_on, emailAge_reason_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_reason_value as emailAge_reason_value,
    element_at(array_sort(array_agg(CASE WHEN emailAge_riskBand_id is not null then struct(ocurred_on, emailAge_riskBand_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_riskBand_id as emailAge_riskBand_id,
    element_at(array_sort(array_agg(CASE WHEN emailAge_riskBand_value is not null then struct(ocurred_on, emailAge_riskBand_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_riskBand_value as emailAge_riskBand_value,
    element_at(array_sort(array_agg(CASE WHEN emailAge_score is not null then struct(ocurred_on, emailAge_score) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_score as emailAge_score,
    element_at(array_sort(array_agg(CASE WHEN emailAge_socialMediaFriends is not null then struct(ocurred_on, emailAge_socialMediaFriends) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_socialMediaFriends as emailAge_socialMediaFriends,
    element_at(array_sort(array_agg(CASE WHEN emailAge_sourceIndustry is not null then struct(ocurred_on, emailAge_sourceIndustry) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_sourceIndustry as emailAge_sourceIndustry,
    element_at(array_sort(array_agg(CASE WHEN emailAge_status_id is not null then struct(ocurred_on, emailAge_status_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_status_id as emailAge_status_id,
    element_at(array_sort(array_agg(CASE WHEN emailAge_status_value is not null then struct(ocurred_on, emailAge_status_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_status_value as emailAge_status_value,
    element_at(array_sort(array_agg(CASE WHEN emailAge_title is not null then struct(ocurred_on, emailAge_title) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).emailAge_title as emailAge_title,
    element_at(array_sort(array_agg(CASE WHEN metadata_context_traceId is not null then struct(ocurred_on, metadata_context_traceId) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).metadata_context_traceId as metadata_context_traceId,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    application_id
                       
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
this: silver.f_kyc_emailage_co
country: co
silver_table_name: f_kyc_emailage_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'emailAge_advice_id', 'emailAge_advice_value', 'emailAge_birthDate', 'emailAge_company', 'emailAge_country', 'emailAge_domain_age', 'emailAge_domain_creationDays', 'emailAge_domain_exits', 'emailAge_domain_name', 'emailAge_domain_riskLevel', 'emailAge_domain_riskLevelId', 'emailAge_eName', 'emailAge_email_age', 'emailAge_email_creationDays', 'emailAge_email_exists', 'emailAge_email_value', 'emailAge_firstSeenDays', 'emailAge_firstVerificationDate', 'emailAge_fraudType', 'emailAge_hits_total', 'emailAge_hits_unique', 'emailAge_imageUrl', 'emailAge_lastVerificationDate', 'emailAge_lastflaggedon', 'emailAge_location', 'emailAge_reason_id', 'emailAge_reason_value', 'emailAge_riskBand_id', 'emailAge_riskBand_value', 'emailAge_score', 'emailAge_socialMediaFriends', 'emailAge_sourceIndustry', 'emailAge_status_id', 'emailAge_status_value', 'emailAge_title', 'metadata_context_traceId', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'emailageobtained': {'direct_attributes': ['application_id', 'client_id', 'emailAge_advice_id', 'emailAge_advice_value', 'emailAge_birthDate', 'emailAge_company', 'emailAge_country', 'emailAge_domain_age', 'emailAge_domain_creationDays', 'emailAge_domain_exits', 'emailAge_domain_name', 'emailAge_domain_riskLevel', 'emailAge_domain_riskLevelId', 'emailAge_eName', 'emailAge_email_age', 'emailAge_email_creationDays', 'emailAge_email_exists', 'emailAge_email_value', 'emailAge_firstSeenDays', 'emailAge_firstVerificationDate', 'emailAge_fraudType', 'emailAge_hits_total', 'emailAge_hits_unique', 'emailAge_imageUrl', 'emailAge_lastVerificationDate', 'emailAge_lastflaggedon', 'emailAge_location', 'emailAge_reason_id', 'emailAge_reason_value', 'emailAge_riskBand_id', 'emailAge_riskBand_value', 'emailAge_score', 'emailAge_socialMediaFriends', 'emailAge_sourceIndustry', 'emailAge_status_id', 'emailAge_status_value', 'emailAge_title', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['emailageobtained']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
