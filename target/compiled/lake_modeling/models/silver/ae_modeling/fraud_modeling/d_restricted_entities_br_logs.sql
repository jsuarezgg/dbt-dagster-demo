
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
restrictedentitysuspected_br AS ( 
    SELECT *
    FROM bronze.restrictedentitysuspected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,restrictedentityblocked_br AS ( 
    SELECT *
    FROM bronze.restrictedentityblocked_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,restrictedentityactivated_br AS ( 
    SELECT *
    FROM bronze.restrictedentityactivated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        client_id,ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,
    event_name,
    event_id
    FROM restrictedentitysuspected_br
    UNION ALL
    SELECT 
        client_id,ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,
    event_name,
    event_id
    FROM restrictedentityblocked_br
    UNION ALL
    SELECT 
        client_id,ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,
    event_name,
    event_id
    FROM restrictedentityactivated_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    client_id,ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,
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
this: silver.d_restricted_entities_br_logs
country: br
silver_table_name: d_restricted_entities_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['client_id', 'event_id', 'ocurred_on', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'restrictedentitysuspected': {'direct_attributes': ['event_id', 'surrogate_key', 'client_id', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'ocurred_on'], 'custom_attributes': {}}, 'restrictedentityblocked': {'direct_attributes': ['event_id', 'surrogate_key', 'client_id', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'ocurred_on'], 'custom_attributes': {}}, 'restrictedentityactivated': {'direct_attributes': ['event_id', 'surrogate_key', 'client_id', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['restrictedentitysuspected', 'restrictedentityblocked', 'restrictedentityactivated']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
