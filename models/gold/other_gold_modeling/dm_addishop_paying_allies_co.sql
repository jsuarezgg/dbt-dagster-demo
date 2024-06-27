{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- QUERY

WITH first_table AS (
SELECT ally_slug,
       start_date_validity,
       end_date_validity,
       hours_for_attribution,
       ally_shop_slug_associated,
       SPLIT(REPLACE(REPLACE(REPLACE(case when grouped_allies = "[]" THEN NULL ELSE grouped_allies END,'"',""),'[',""),']',""),',') grouped_allies,
       NVL(CASE WHEN grouped_allies = "[]" THEN NULL ELSE grouped_allies END,-99) AS flag_grouped_allies,
       NVL(addishop_fee, -99) AS addi_shop_fee, 
       NVL(addishop_active, -99) AS addi_shop_active,
       NVL(LAG(addishop_fee) OVER(PARTITION BY ally_slug ORDER BY start_date_validity, id), -99) AS previous_fee,
       NVL(LAG(addishop_active) OVER(PARTITION BY ally_slug ORDER BY start_date_validity, id), -99) AS previous_active,
       NVL(LAG(hours_for_attribution) OVER(PARTITION BY ally_slug ORDER BY start_date_validity, id), -99) AS previous_hours_for_attribution,
      NVL(LAG( case when grouped_allies = "[]" THEN NULL ELSE grouped_allies END) OVER(PARTITION BY ally_slug ORDER BY start_date_validity, id),-99) AS previous_grouped_allies
FROM {{ ref('d_ally_management_ally_addishop_configurations_co') }}
)
, filter_table AS (
SELECT ally_slug,
       start_date_validity AS start_date,
       end_date_validity AS end_date,
       grouped_allies,
       hours_for_attribution,
       ally_shop_slug_associated,
       addi_shop_fee AS lead_gen_fee, 
       addi_shop_active,
       previous_fee,
       previous_active,
       CASE WHEN addi_shop_active <> previous_active OR (addi_shop_active IS true AND addi_shop_fee <> previous_fee) OR (flag_grouped_allies <> previous_grouped_allies)
        OR (hours_for_attribution <> previous_hours_for_attribution) THEN 1
            ELSE 0
       END addi_shop_flag
FROM first_table
)
, flag_table AS (
SELECT a.ally_slug, 
       CASE WHEN a.start_date >= '2023-03-02' THEN a.start_date ELSE b.start_date END AS start_date,
       CASE WHEN a.start_date >= '2023-03-02' THEN COALESCE(a.end_date,'2050-01-01') ELSE b.end_date END AS end_date,
       grouped_allies,
       hours_for_attribution,
       ally_shop_slug_associated,
       a.lead_gen_fee,
       a.addi_shop_active,
       CASE WHEN a.addi_shop_active = true 
              AND (LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS NULL 
              OR (LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) = -99
              OR  LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS false
              OR  LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS true))
              THEN 'opt-in'
            WHEN a.addi_shop_active = false 
              AND LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS true 
              THEN 'opt-out'
            ELSE null
       END flag_active
FROM filter_table a
LEFT JOIN {{ source('silver', 'd_marketplace_lead_gen_fee_allies_co_legacy') }} b ON b.ally_name = a.ally_slug AND b.start_date < '2023-03-02'
WHERE addi_shop_flag = 1
UNION ALL
SELECT ally_name AS ally_slug,
       start_date,
       end_date,
       NULL AS grouped_allies,
       NULL AS hours_for_attribution,
       NULL AS ally_shop_slug_associated,
       lead_gen_fee,
       NULL AS addi_shop_active,
       'opt-in' AS flag_active
FROM {{ source('silver', 'd_marketplace_lead_gen_fee_allies_co_legacy') }}
WHERE end_date < '2023-03-02' 
)
, status_table AS (
SELECT ally_slug,
       start_date,
       CASE WHEN end_date >= '2023-03-02' THEN COALESCE(LEAD(start_date) OVER(PARTITION BY ally_slug ORDER BY start_date), '2050-01-01') 
            ELSE end_date
       END AS end_date,
       grouped_allies,
       hours_for_attribution,
       ally_shop_slug_associated,
       lead_gen_fee,
       flag_active,
       CURRENT_DATE() AS ingested_at
FROM flag_table
)
, fixed_start_end_date AS (
SELECT DISTINCT 
       st.ally_slug,
       ac.ally_cluster,
       ac.ally_brand,
       CASE WHEN das.ally_channel ILIKE '%COMM%' THEN 'E_COMMERCE' ELSE 'PAYLINK' END AS channel,
       --We have some start_date overlaping because of a new config related to in-store slugs. We need to manually fix.
       CASE WHEN st.ally_slug = 'decathlon-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:53:55.386'
              WHEN st.ally_slug = 'lecoqsportif-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:53:42.629'
              WHEN st.ally_slug = 'lineaestetica-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:07:13.803'
              WHEN st.ally_slug = 'medipiel-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:49:39.455'
              WHEN st.ally_slug = 'pinkrose-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:50:06.290'
              WHEN st.ally_slug = 'socoda-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:52:55.494'
              WHEN st.ally_slug = 'springstep-ecommerce' AND date_trunc('day', st.start_date) = '2023-12-06' THEN '2023-12-04 21:54:08.401'
       ELSE start_date END AS start_date,

       --We have some end_dates overlaping because of a new config related to in-store slugs. We need to manually fix.
       CASE WHEN st.ally_slug = 'decathlon-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:53:55.385'
              WHEN st.ally_slug = 'lecoqsportif-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:53:42.628'
              WHEN st.ally_slug = 'lineaestetica-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:07:13.802'
              WHEN st.ally_slug = 'medipiel-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:49:39.454'
              WHEN st.ally_slug = 'pinkrose-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:50:06.289'
              WHEN st.ally_slug = 'socoda-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:52:55.493'
              WHEN st.ally_slug = 'springstep-ecommerce' AND date_trunc('day', st.end_date) = '2023-12-06' THEN '2023-12-04 21:54:08.400'
       ELSE end_date END AS end_date,
       st.grouped_allies,
       st.hours_for_attribution,
       st.ally_shop_slug_associated,
       st.lead_gen_fee,
       ingested_at
FROM status_table st
 LEFT JOIN {{ ref('bl_ally_brand_ally_slug_status') }} ac
        ON st.ally_slug = ac.ally_slug
        AND ac.country_code = 'CO'
LEFT JOIN {{ ref('d_ally_slugs_co') }} das
  ON das.ally_slug = st.ally_slug
WHERE 1=1 
       AND flag_active = 'opt-in'
       AND st.ally_slug NOT IN ('addi', 'adelante', 'additest')

)
SELECT
       f.ally_slug,
       f.ally_cluster,
       f.ally_brand,
       f.channel,
       f.start_date,
       f.end_date, 
       f.grouped_allies,
       f.hours_for_attribution,
       f.ally_shop_slug_associated,
       f.lead_gen_fee,
       CASE WHEN CURRENT_DATE() < end_date OR end_date IS NULL THEN 'Active' ELSE 'Inactive' END AS current_status,
       ingested_at
FROM fixed_start_end_date f
WHERE 1=1 
       AND start_date < end_date

