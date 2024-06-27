
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
identityphotosthirdpartyapproved_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyapproved_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscarded_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscarded_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrejected_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrejected_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrequested_unnested_by_truora_event_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrequested_unnested_by_truora_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
    event_name,
    event_id
    FROM identityphotosthirdpartyapproved_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
    event_name,
    event_id
    FROM identityphotosthirdpartydiscarded_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
    event_name,
    event_id
    FROM identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
    event_name,
    event_id
    FROM identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
    event_name,
    event_id
    FROM identityphotosthirdpartyrejected_unnested_by_truora_event_co
    UNION ALL
    SELECT 
        application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
    event_name,
    event_id
    FROM identityphotosthirdpartyrequested_unnested_by_truora_event_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,ocurred_on,surrogate_key,truora_event_confidence_score,truora_event_declined_reason,truora_event_failure_status,truora_event_threshold,truora_event_timestamp,truora_event_type,truora_event_validation_id,truora_event_validation_status,
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
this: silver.f_identity_photos_third_party_truora_events_co_logs
country: co
silver_table_name: f_identity_photos_third_party_truora_events_co_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'ocurred_on', 'surrogate_key', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'truora_event_type', 'truora_event_validation_id', 'truora_event_validation_status']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'identityphotosthirdpartyapproved_unnested_by_truora_event': {'direct_attributes': ['surrogate_key', 'truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscarded_unnested_by_truora_event': {'direct_attributes': ['surrogate_key', 'truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event': {'direct_attributes': ['surrogate_key', 'truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event': {'direct_attributes': ['surrogate_key', 'truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrejected_unnested_by_truora_event': {'direct_attributes': ['surrogate_key', 'truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrequested_unnested_by_truora_event': {'direct_attributes': ['surrogate_key', 'truora_event_validation_id', 'application_id', 'client_id', 'truora_event_type', 'truora_event_validation_status', 'truora_event_confidence_score', 'truora_event_declined_reason', 'truora_event_failure_status', 'truora_event_threshold', 'truora_event_timestamp', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['identityphotosthirdpartyapproved_unnested_by_truora_event', 'identityphotosthirdpartydiscarded_unnested_by_truora_event', 'identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event', 'identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event', 'identityphotosthirdpartyrejected_unnested_by_truora_event', 'identityphotosthirdpartyrequested_unnested_by_truora_event']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
