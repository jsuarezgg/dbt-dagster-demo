{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
SELECT
  ap.addi_product_id,
  ap.addi_product_name,
  ap.addi_product_stage,
  ap.addi_product_interested,
  ap.addi_product_lgf,
  ap.addi_product_mdf,
  ap.addi_product_is_deleted,
  ap.addi_product_link,
  ap.addi_product_created_date,
  ap.addi_product_systemmodstamp,
  ap.addi_product_lastmoddate,
  ap.opportunity_addi_products_link,
  ap.addi_product_created_by_id,
  uc.user_name AS addi_product_created_by_name,
  ap.addi_product_last_modified_by_id,
  ulm.user_name AS addi_product_last_modified_by_name,
  ap.opportunity_id,
  o.opportunity_slug,
  o.opportunity_is_deleted,
  o.opportunity_link,
  -- MANDATORY FIELDS
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at,
  ap.airbyte_emitted_at AS addi_product_airbyte_emitted_at,
  ap.ingested_from_s3_at AS addi_product_ingested_from_s3_at
FROM      {{ ref('salesforce_custom_addi_products')}} AS ap
LEFT JOIN {{ ref('salesforce_opportunity')}}          AS o   ON ap.opportunity_id                   = o.opportunity_id
LEFT JOIN {{ ref('salesforce_user')}}                 AS uc  ON ap.addi_product_created_by_id       = uc.user_id
LEFT JOIN {{ ref('salesforce_user')}}                 AS ulm ON ap.addi_product_last_modified_by_id = ulm.user_id
