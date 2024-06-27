
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
identityphotosthirdpartyapproved_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscarded_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartymanualverificationrequired_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartymanualverificationrequired_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrejected_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrequested_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrequested_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartystarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartystarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartyapproved_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartydiscarded_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartydiscardedbyrisk_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartymanualverificationrequired_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartyrejected_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartyrequested_co
    UNION ALL
    SELECT 
        application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
    event_name,
    event_id
    FROM identityphotosthirdpartystarted_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,identity_photos_tp_custom_last_status,idv_provider,idv_third_party_attempts,ocurred_on,
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
this: silver.f_identity_photos_third_party_co_logs
country: co
silver_table_name: f_identity_photos_third_party_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'identityphotosthirdpartyapproved': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscarded': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscardedbyrisk': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartymanualverificationrequired': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrejected': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartyrequested': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}, 'identityphotosthirdpartystarted': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'identity_photos_tp_custom_last_status', 'idv_provider', 'idv_third_party_attempts', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['identityphotosthirdpartyapproved', 'identityphotosthirdpartydiscarded', 'identityphotosthirdpartydiscardedbyrisk', 'identityphotosthirdpartymanualverificationrequired', 'identityphotosthirdpartyrejected', 'identityphotosthirdpartyrequested', 'identityphotosthirdpartystarted']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
