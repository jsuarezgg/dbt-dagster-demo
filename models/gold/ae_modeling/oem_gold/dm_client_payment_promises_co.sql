{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH
kustomer_promises AS (
SELECT cac.client_id,
split_part(kcca.custom_attribute_value,'.',1) AS kustomer_collections_resol_tree1,
split_part(kcca.custom_attribute_value,'.',2) AS kustomer_collections_resol_tree2,
split_part(kcca.custom_attribute_value,'.',3) AS kustomer_collections_resol_tree3,
kcca.reference_date 
FROM {{ ref('f_kustomer_conversations_custom_attributes') }} kcca
LEFT JOIN {{ ref('f_kustomer_conversations') }} kc ON kc.conversation_id=kcca.conversation_id 
LEFT JOIN {{ ref('d_kustomer_crm_aggregator_clients_co') }} cac ON cac.kustomer_id=kc.customer_id
WHERE kcca.custom_attribute_name='resolutionCollectionsTree'
)

, client_payment_promises_co AS ( 
SELECT cpp.*, 
       COUNT(coalesce(cp.payment_id,cpp.client_payment_id)) AS payments_num,
       COLLECT_SET(named_struct('payment_id',coalesce(cp.payment_id,cpp.client_payment_id),'payment_amount',cp.amount)) as payments_details
FROM {{ ref('f_client_payment_promises_co') }} cpp
LEFT JOIN {{ ref('f_client_payments_client_payments_co') }} cp ON cp.client_id = cpp.client_id 
                                                               AND cp.payment_date BETWEEN cpp.start_date AND cpp.end_date
GROUP BY ALL   
)

SELECT cpp.*,
kp.kustomer_collections_resol_tree1,
kp.kustomer_collections_resol_tree2,
kp.kustomer_collections_resol_tree3
FROM client_payment_promises_co cpp
LEFT JOIN kustomer_promises kp ON kp.client_id=cpp.client_id AND kp.reference_date::date=cpp.start_date::date
