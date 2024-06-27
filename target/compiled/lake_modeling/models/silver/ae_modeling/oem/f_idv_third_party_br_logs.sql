
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
idvthirdpartyapproved_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartycollected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartycollected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyrequested_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyrequested_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartystarted_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartystarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartymanualverificationrequired_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartymanualverificationrequired_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyphotorejected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyphotorejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyrejected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyrejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyskipped_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyskipped_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartyapproved_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartycollected_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartyrequested_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,NULL as idv_tp_unico_error_code,NULL as idv_tp_unico_process_id,NULL as idv_tp_unico_score,NULL as idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartystarted_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartymanualverificationrequired_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartyphotorejected_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartyrejected_br
    UNION ALL
    SELECT 
        application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
    event_name,
    event_id
    FROM idvthirdpartyskipped_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,idv_tp_custom_last_status,idv_tp_provider,idv_tp_unico_error_code,idv_tp_unico_process_id,idv_tp_unico_score,idv_tp_unico_status,ocurred_on,
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
this: silver.f_idv_third_party_br_logs
country: br
silver_table_name: f_idv_third_party_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'idv_tp_custom_last_status', 'idv_tp_provider', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'idvthirdpartyapproved': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartycollected': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyrequested': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartystarted': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartymanualverificationrequired': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyphotorejected': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyrejected': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}, 'idvthirdpartyskipped': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'idv_tp_custom_last_status', 'idv_tp_unico_error_code', 'idv_tp_unico_process_id', 'idv_tp_unico_score', 'idv_tp_unico_status', 'idv_tp_provider', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['idvthirdpartyapproved', 'idvthirdpartycollected', 'idvthirdpartyrequested', 'idvthirdpartystarted', 'idvthirdpartymanualverificationrequired', 'idvthirdpartyphotorejected', 'idvthirdpartyrejected', 'idvthirdpartyskipped']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
