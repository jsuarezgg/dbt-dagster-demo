
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
pixpaymentrefunded_br AS ( 
    SELECT *
    FROM bronze.pixpaymentrefunded_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,client_id,custom_event_domain,custom_payment_refund_status,ocurred_on,pix_payment_amount,pix_payment_number,pix_payment_refund_occurred_on,pix_payment_refund_reason,user_id,
    event_name,
    event_id
    FROM pixpaymentrefunded_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,client_id,custom_event_domain,custom_payment_refund_status,ocurred_on,pix_payment_amount,pix_payment_number,pix_payment_refund_occurred_on,pix_payment_refund_reason,user_id,
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
this: silver.f_pix_payment_refunds_br_logs
country: br
silver_table_name: f_pix_payment_refunds_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'client_id', 'custom_event_domain', 'custom_payment_refund_status', 'event_id', 'ocurred_on', 'pix_payment_amount', 'pix_payment_number', 'pix_payment_refund_occurred_on', 'pix_payment_refund_reason', 'user_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'pixpaymentrefunded': {'direct_attributes': ['event_id', 'pix_payment_number', 'ally_slug', 'client_id', 'user_id', 'pix_payment_amount', 'pix_payment_refund_occurred_on', 'pix_payment_refund_reason', 'custom_event_domain', 'custom_payment_refund_status', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['pixpaymentrefunded']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
