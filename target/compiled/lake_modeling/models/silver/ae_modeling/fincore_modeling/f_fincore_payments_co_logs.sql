
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientpaymentannulledv2_co AS ( 
    SELECT *
    FROM bronze.clientpaymentannulledv2_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentreceivedv3_co AS ( 
    SELECT *
    FROM bronze.clientpaymentreceivedv3_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,wompitransactioncreated_co AS ( 
    SELECT *
    FROM bronze.wompitransactioncreated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,wompitransactionupdated_co AS ( 
    SELECT *
    FROM bronze.wompitransactionupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        annulment_reason,client_id,NULL as custom_is_wompi,is_annulled,ocurred_on,NULL as paid_amount,NULL as payment_date,payment_id,NULL as payment_method,NULL as wompi_business_agreement_code,NULL as wompi_payment_intention_identifier,NULL as wompi_transaction_status,NULL as wompi_transaction_status_reason,
    event_name,
    event_id
    FROM clientpaymentannulledv2_co
    UNION ALL
    SELECT 
        annulment_reason,client_id,NULL as custom_is_wompi,is_annulled,ocurred_on,paid_amount,payment_date,payment_id,payment_method,NULL as wompi_business_agreement_code,NULL as wompi_payment_intention_identifier,NULL as wompi_transaction_status,NULL as wompi_transaction_status_reason,
    event_name,
    event_id
    FROM clientpaymentreceivedv3_co
    UNION ALL
    SELECT 
        NULL as annulment_reason,client_id,custom_is_wompi,NULL as is_annulled,ocurred_on,NULL as paid_amount,NULL as payment_date,payment_id,NULL as payment_method,NULL as wompi_business_agreement_code,NULL as wompi_payment_intention_identifier,wompi_transaction_status,wompi_transaction_status_reason,
    event_name,
    event_id
    FROM wompitransactioncreated_co
    UNION ALL
    SELECT 
        NULL as annulment_reason,client_id,custom_is_wompi,NULL as is_annulled,ocurred_on,NULL as paid_amount,NULL as payment_date,payment_id,NULL as payment_method,wompi_business_agreement_code,wompi_payment_intention_identifier,wompi_transaction_status,wompi_transaction_status_reason,
    event_name,
    event_id
    FROM wompitransactionupdated_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    annulment_reason,client_id,custom_is_wompi,is_annulled,ocurred_on,paid_amount,payment_date,payment_id,payment_method,wompi_business_agreement_code,wompi_payment_intention_identifier,wompi_transaction_status,wompi_transaction_status_reason,
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
this: silver.f_fincore_payments_co_logs
country: co
silver_table_name: f_fincore_payments_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['annulment_reason', 'client_id', 'custom_is_wompi', 'event_id', 'is_annulled', 'ocurred_on', 'paid_amount', 'payment_date', 'payment_id', 'payment_method', 'wompi_business_agreement_code', 'wompi_payment_intention_identifier', 'wompi_transaction_status', 'wompi_transaction_status_reason']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ClientPaymentAnnulledV2': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'payment_id', 'is_annulled', 'annulment_reason', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'ClientPaymentReceivedV3': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'payment_id', 'client_id', 'payment_method', 'paid_amount', 'payment_date', 'is_annulled', 'annulment_reason', 'ocurred_on'], 'custom_attributes': {}}, 'WompiTransactionCreated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'payment_id', 'client_id', 'wompi_transaction_status', 'wompi_transaction_status_reason', 'custom_is_wompi', 'ocurred_on'], 'custom_attributes': {}}, 'WompiTransactionUpdated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'payment_id', 'client_id', 'wompi_transaction_status', 'wompi_transaction_status_reason', 'wompi_business_agreement_code', 'wompi_payment_intention_identifier', 'custom_is_wompi', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ClientPaymentAnnulledV2', 'ClientPaymentReceivedV3', 'WompiTransactionCreated', 'WompiTransactionUpdated']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
