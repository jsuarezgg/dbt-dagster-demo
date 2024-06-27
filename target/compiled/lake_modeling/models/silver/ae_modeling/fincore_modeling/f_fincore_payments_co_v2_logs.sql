
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientpaymentannulledv2_co AS ( 
    SELECT *
    FROM bronze.clientpaymentannulledv2_co 
)
,clientpaymentreceivedv3_co AS ( 
    SELECT *
    FROM bronze.clientpaymentreceivedv3_co 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        annulment_reason,client_id,is_annulled,ocurred_on,NULL as paid_amount,NULL as payment_date,payment_id,NULL as payment_method,
    event_name,
    event_id
    FROM clientpaymentannulledv2_co
    UNION ALL
    SELECT 
        annulment_reason,client_id,is_annulled,ocurred_on,paid_amount,payment_date,payment_id,payment_method,
    event_name,
    event_id
    FROM clientpaymentreceivedv3_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    annulment_reason,client_id,is_annulled,ocurred_on,paid_amount,payment_date,payment_id,payment_method,
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
is_incremental: False
this: silver.f_fincore_payments_co_v2_logs
country: co
silver_table_name: f_fincore_payments_co_v2_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['annulment_reason', 'client_id', 'event_id', 'is_annulled', 'ocurred_on', 'paid_amount', 'payment_date', 'payment_id', 'payment_method']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ClientPaymentAnnulledV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'payment_id', 'is_annulled', 'annulment_reason', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'ClientPaymentReceivedV3': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'payment_id', 'client_id', 'payment_method', 'paid_amount', 'payment_date', 'is_annulled', 'annulment_reason', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ClientPaymentAnnulledV2', 'ClientPaymentReceivedV3']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
