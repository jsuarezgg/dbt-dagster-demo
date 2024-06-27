{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
  a.country_code,
  a.application_id,
  a.client_id,
  a.application_datetime,
  a.application_datetime_local,
  a.application_date_local,
  a.application_channel,
  a.synthetic_channel,
  a.ally_slug,
  a.store_slug,
  a.ally_brand,
  a.ally_vertical,
  a.ally_cluster,
  a.order_id,
  a.client_type,
  a.custom_platform_version,
  a.journey_name,
  a.original_product,
  a.processed_product,
  a.synthetic_product_category,
  a.synthetic_product_subcategory,
  a.requested_amount,
  a.requested_amount_without_discount,
  a.synthetic_requested_amount,
  a.last_event_type_prime,
  a.last_journey_stage_name_prime,
  a.last_event_name_prime,
  a.last_event_id_prime,
  rmt.pse_invoice_requested,
  rmt.pse_invoice_confirmed,
  rmt.payment_confirmed,
  p.payment_financial_institution_name, 
  p.payment_status, 
  p.payment_status_reason,
  a.campaign_id,
  a.store_user_id,
  a.status_ally_brand,
  a.status_ally_slug,
  a.is_using_preapproval_proxy,
  a.preapproval_amount,
  a.preapproval_expiration_date,
  a.custom_is_preapproval_completed,
  a.custom_is_bnpn_branched,
  a.custom_is_checkpoint_application,
  a.custom_is_preapproval_application,
  a.custom_is_santander_branched,
  a.declination_reason,
  a.declination_comments_redacted,
  a.device_id,
  a.is_addishop_referral,
  a.addishop_channel,
  a.is_addishop_referral_paid,
  CASE WHEN b.application_id IS NULL THEN FALSE ELSE TRUE END AS is_origination,
  b.gmv,
--  b.is_addishop_referral,
--  b.is_addishop_referral_paid,
--  b.addishop_channel,
  b.lead_gen_fee_rate,
  b.lead_gen_fee_amount,
  b.ally_mdf,
  b.mdf_amount,
  fr.price AS fx_rate,
  CASE WHEN c.transaction_id IS NOT NULL THEN TRUE ELSE FALSE END AS cancelled,
  c.custom_transaction_cancellation_status,
  c.transaction_cancellation_reason,
  c.ocurred_on_date AS cancelled_date,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM  {{ ref('dm_applications') }} a
LEFT JOIN {{ ref('dm_originations') }} b
  ON a.application_id = b.application_id
LEFT JOIN (SELECT
              application_id,
              CASE WHEN lower(stages) ILIKE '%pseinvoicerequestedco%' THEN 1 ELSE 0 END AS pse_invoice_requested,
              CASE WHEN lower(stages) ILIKE '%pseinvoiceconfirmedco%' THEN 1 ELSE 0 END AS pse_invoice_confirmed,
              CASE WHEN lower(stages) ILIKE '%bnpnpaymentapprovedco%' THEN 1 ELSE 0 END AS payment_confirmed
            FROM {{ ref('risk_master_table_co') }}
            WHERE product = 'BNPN_CO') rmt
  ON rmt.application_id = a.application_id
LEFT JOIN {{ ref('f_transaction_cancellations_co') }} c
    ON c.transaction_id = a.application_id
LEFT JOIN {{ ref('f_applications_pse_payment_co') }} p
    ON a.application_id = p.application_id
LEFT JOIN silver.d_fx_rate fr 
  ON fr.country_code = a.country_code 
  AND fr.is_active IS true
WHERE 1=1
  AND a.original_product = 'BNPN_CO'
  AND a.country_code = 'CO'