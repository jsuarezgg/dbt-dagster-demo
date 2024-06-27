
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
applicationcreated_unnested_by_items_co AS ( 
    SELECT *
    FROM bronze.applicationcreated_unnested_by_items_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,application_id,channel,client_id,item_brand,item_category,item_discount,item_name,item_pictureurl,item_quantity,item_sku,item_taxamount,item_unitprice,ocurred_on,order_id,product,surrogate_key,
    event_name,
    event_id
    FROM applicationcreated_unnested_by_items_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_id,channel,client_id,item_brand,item_category,item_discount,item_name,item_pictureurl,item_quantity,item_sku,item_taxamount,item_unitprice,ocurred_on,order_id,product,surrogate_key,
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
this: silver.f_applications_order_details_co_logs
country: co
silver_table_name: f_applications_order_details_co_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_id', 'channel', 'client_id', 'event_id', 'event_name', 'item_brand', 'item_category', 'item_discount', 'item_name', 'item_pictureurl', 'item_quantity', 'item_sku', 'item_taxamount', 'item_unitprice', 'ocurred_on', 'order_id', 'product', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ApplicationCreated_unnested_by_items': {'stage': 'GLOBAL', 'direct_attributes': ['surrogate_key', 'event_name', 'event_id', 'ocurred_on', 'application_id', 'client_id', 'ally_slug', 'channel', 'order_id', 'product', 'item_brand', 'item_category', 'item_discount', 'item_name', 'item_pictureurl', 'item_quantity', 'item_sku', 'item_taxamount', 'item_unitprice'], 'custom_attributes': {}}}
events_keys: ['ApplicationCreated_unnested_by_items']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
