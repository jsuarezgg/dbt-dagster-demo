
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
        ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM restrictedentitysuspected_br
    UNION ALL
    SELECT 
        ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM restrictedentityblocked_br
    UNION ALL
    SELECT 
        ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM restrictedentityactivated_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    last_event_ocurred_on_processed as ocurred_on,restricted_entity_created_at,restricted_entity_journey,restricted_entity_reason,restricted_entity_reference,restricted_entity_source,restricted_entity_status,restricted_entity_type,restricted_entity_value,surrogate_key,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.d_restricted_entities_br  
    WHERE 
    silver.d_restricted_entities_br.surrogate_key IN (SELECT DISTINCT surrogate_key FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    surrogate_key,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_created_at is not null then struct(ocurred_on, restricted_entity_created_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_created_at as restricted_entity_created_at,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_journey is not null then struct(ocurred_on, restricted_entity_journey) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_journey as restricted_entity_journey,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_reason is not null then struct(ocurred_on, restricted_entity_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_reason as restricted_entity_reason,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_reference is not null then struct(ocurred_on, restricted_entity_reference) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_reference as restricted_entity_reference,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_source is not null then struct(ocurred_on, restricted_entity_source) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_source as restricted_entity_source,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_status is not null then struct(ocurred_on, restricted_entity_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_status as restricted_entity_status,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_type is not null then struct(ocurred_on, restricted_entity_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_type as restricted_entity_type,
    element_at(array_sort(array_agg(CASE WHEN restricted_entity_value is not null then struct(ocurred_on, restricted_entity_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).restricted_entity_value as restricted_entity_value,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    surrogate_key
                       
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
this: silver.d_restricted_entities_br
country: br
silver_table_name: d_restricted_entities_br
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['ocurred_on', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'restrictedentitysuspected': {'direct_attributes': ['surrogate_key', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'ocurred_on'], 'custom_attributes': {}}, 'restrictedentityblocked': {'direct_attributes': ['surrogate_key', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'ocurred_on'], 'custom_attributes': {}}, 'restrictedentityactivated': {'direct_attributes': ['surrogate_key', 'restricted_entity_created_at', 'restricted_entity_journey', 'restricted_entity_reason', 'restricted_entity_reference', 'restricted_entity_source', 'restricted_entity_status', 'restricted_entity_type', 'restricted_entity_value', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['restrictedentitysuspected', 'restrictedentityblocked', 'restrictedentityactivated']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
