{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
  a.application_id,
  a.ally_slug,
  a.channel,
  c.ally_vertical,
  c.ally_cluster,
  a.client_id,
  a.client_type,
  CAST(a.last_event_ocurred_on_processed AS DATE) AS ocurred_on_date,
  a.last_event_ocurred_on_processed,
  CASE WHEN dap.application_id IS NULL THEN FALSE ELSE TRUE END AS product_selected_flag,
  b.declination_reason product_selection_declination_reason,
  b.declination_comments AS product_selection_declination_comments,
  original_product,
  requested_amount,
  requested_amount_without_discount,
  addishop_channel,
  is_addishop_referral,
  is_addishop_referral_paid,
  auto_selection
FROM {{ source('silver_live', 'f_product_selection_events_co') }} AS a
LEFT JOIN  {{ ref('dm_applications') }}  dap
  ON a.application_id = dap.application_id
  AND dap.country_code = 'CO'
LEFT JOIN  {{ source('silver_live', 'f_product_selection_declination_data_co') }} b
  ON a.application_id = b.application_id
LEFT JOIN {{ ref('bl_ally_brand_ally_slug_status') }} AS c
  ON c.country_code = 'CO'
  AND c.ally_slug = a.ally_slug