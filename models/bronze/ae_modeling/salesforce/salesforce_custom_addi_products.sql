{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_addi_products_full_refresh_overwrite
SELECT
    -- Custom Object attributes
	Id AS addi_product_id,
    COALESCE(Product__c, Name) AS addi_product_name,
    Stage__c AS addi_product_stage,
    Interested__c AS addi_product_interested,
    LGF__c AS addi_product_lgf,
    MDF__c AS addi_product_mdf,
    IsDeleted AS addi_product_is_deleted,
    CONCAT("https://addi.lightning.force.com/lightning/r/Addi_Products__c/",Id,"/view") AS addi_product_link,
    Opportunity__c AS opportunity_id,
    CONCAT("https://addi.lightning.force.com/lightning/r/Opportunity/",Opportunity__c,"/related/Addi_Products__r/view") AS opportunity_addi_products_link,
    CreatedById AS addi_product_created_by_id,
    CreatedDate.member0 AS addi_product_created_date,
    SystemModstamp.member0 AS addi_product_systemmodstamp,
    LastModifiedDate.member0 AS addi_product_lastmoddate,
    LastModifiedById AS addi_product_last_modified_by_id,
    -- Object Timestamps
    Active_Timestamp__c.member0::TIMESTAMP AS addi_product_active_timestamp,
    Discovery_Timestamp__c.member0::TIMESTAMP AS addi_product_discovery_timestamp,
    Onboarding_Timestamp__c.member0::TIMESTAMP AS addi_product_onboarding_timestamp,
    Sales_Pitch_Timestamp__c.member0::TIMESTAMP AS addi_product_sales_pitch_timestamp,
    Churned_Timestamp__c.member0::TIMESTAMP AS addi_product_churned_timestamp,
    Negotiation_Timestamp__c.member0::TIMESTAMP AS addi_product_negotiation_timestamp,
    Contract_Signing_Timestamp__c.member0::TIMESTAMP AS addi_product_contract_signing_timestamp,
    -- Custom Object - Custom attributes
    Cual__c AS addi_product_cual,
    Afiliado__c AS addi_product_afiliado,
    Que_CMS_tiene__c AS addi_product_que_cms_tiene,
    PSE_en_su_Ecomm__c AS addi_product_pse_en_su_ecomm,
    Next_Steps_field__c AS addi_product_next_steps_field,
    Trafico_para_Tienda__c AS addi_product_trafico_para_tienda,
    Comission_con_esos_MP__c AS addi_product_comission_con_esos_mp,
    Cual_integracion_del_PSE__c AS addi_product_cual_integracion_del_pse,
    Metrica_RetornoInversion__c AS addi_product_metrica_retorno_inversion,
    Usa_estrategias_para_CPA__c AS addi_product_usa_estrategias_para_cpa,
    Estrategias_para_Ecommerce__c AS addi_product_estrategias_para_ecommerce,
    Pagos_mixtos_en_una_misma_TRX__c AS addi_product_pagos_mixtos_en_una_misma_trx,
    Campanas_de_Atribucion_en_Ventas__c AS addi_product_campanas_de_atribucion_en_ventas,
    Cuenta_con_exclusividad_con_su_PSE__c AS addi_product_cuenta_con_exclusividad_con_su_pse,
    Que_tarifa_maneja_con_su_PSE_Actual__c AS addi_product_que_tarifa_maneja_con_su_pse_actual,
    Cuanto_es_el_valor_que_te_cobra_PSE_por__c AS addi_product_cuanto_es_el_valor_que_te_cobra_pse_por,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'salesforce_addi_products_full_refresh_overwrite') }}


