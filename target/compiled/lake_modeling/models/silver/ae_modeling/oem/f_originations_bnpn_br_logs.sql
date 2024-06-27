
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
pixpaymentreceivedbr_br AS ( 
    SELECT *
    FROM bronze.pixpaymentreceivedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,application_id,client_id,ocurred_on,origination_date,requested_amount,
    event_name,
    event_id
    FROM pixpaymentreceivedbr_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_id,client_id,ocurred_on,origination_date,requested_amount,
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
this: silver.f_originations_bnpn_br_logs
country: br
silver_table_name: f_originations_bnpn_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_id', 'client_id', 'event_id', 'ocurred_on', 'origination_date', 'requested_amount']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'PixPaymentReceivedBR': {'stage': 'bn_pn_payments_br', 'direct_attributes': ['event_id', 'application_id', 'ally_slug', 'client_id', 'origination_date', 'requested_amount', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['PixPaymentReceivedBR']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
