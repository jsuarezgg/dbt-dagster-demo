
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientaddedtoexclusionlist_co AS ( 
    SELECT *
    FROM bronze.clientaddedtoexclusionlist_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        client_id,loan_id,ocurred_on,reason,
    event_name,
    event_id
    FROM clientaddedtoexclusionlist_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    client_id,loan_id,ocurred_on,reason,
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
this: silver.f_client_exclusions_co_logs
country: co
silver_table_name: f_client_exclusions_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['client_id', 'event_id', 'loan_id', 'ocurred_on', 'reason']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ClientAddedToExclusionList': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'client_id', 'loan_id', 'reason', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ClientAddedToExclusionList']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
