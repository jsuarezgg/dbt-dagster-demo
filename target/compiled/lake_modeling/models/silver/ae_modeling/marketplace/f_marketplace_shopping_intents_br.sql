
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
shoppingintentregistered_br AS ( 
    SELECT *
    FROM bronze.shoppingintentregistered_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,campaign_id,channel,client_id,ocurred_on,shopping_intent_id,device_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM shoppingintentregistered_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,campaign_id,channel,client_id,ocurred_on,shopping_intent_id,device_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,campaign_id,channel,client_id,last_event_ocurred_on_processed as ocurred_on,shopping_intent_id,device_id,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_marketplace_shopping_intents_br  
    WHERE 
    silver.f_marketplace_shopping_intents_br.shopping_intent_id IN (SELECT DISTINCT shopping_intent_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    shopping_intent_id,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN campaign_id is not null then struct(ocurred_on, campaign_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).campaign_id as campaign_id,
    element_at(array_sort(array_agg(CASE WHEN channel is not null then struct(ocurred_on, channel) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).channel as channel,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN device_id is not null then struct(ocurred_on, device_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).device_id as device_id,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    shopping_intent_id
                       
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
this: silver.f_marketplace_shopping_intents_br
country: br
silver_table_name: f_marketplace_shopping_intents_br
table_pk_fields: ['shopping_intent_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'campaign_id', 'channel', 'client_id', 'ocurred_on', 'shopping_intent_id', 'device_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ShoppingIntentRegistered': {'stage': 'shoppingintentregistered_br', 'direct_attributes': ['ally_slug', 'shopping_intent_id', 'client_id', 'campaign_id', 'channel', 'ocurred_on', 'device_id'], 'custom_attributes': {}}}
events_keys: ['ShoppingIntentRegistered']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
