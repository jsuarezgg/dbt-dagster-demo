

-- QUERY

WITH first_table AS (
SELECT ally_slug,
       start_date_validity,
       end_date_validity,
       NVL(data:addiShop.fee, -99) AS addi_shop_fee, 
       NVL(data:addiShop.active, -99) AS addi_shop_active,
       NVL(LAG(data:addiShop.fee) OVER(PARTITION BY ally_slug ORDER BY start_date_validity), -99) AS previous_fee,
       NVL(LAG(data:addiShop.active) OVER(PARTITION BY ally_slug ORDER BY start_date_validity), -99) AS previous_active
FROM silver.d_ally_management_ally_config_co
)
, filter_table AS (
SELECT ally_slug,
       start_date_validity AS start_date,
       end_date_validity AS end_date,
       addi_shop_fee AS lead_gen_fee, 
       addi_shop_active,
       previous_fee,
       previous_active,
       CASE WHEN addi_shop_active <> previous_active OR (addi_shop_active IS true AND addi_shop_fee <> previous_fee) THEN 1
            ELSE 0
       END addi_shop_flag
FROM first_table
)
, flag_table AS (
SELECT a.ally_slug, 
       CASE WHEN a.start_date >= '2023-03-02' THEN a.start_date ELSE b.start_date END AS start_date,
       CASE WHEN a.start_date >= '2023-03-02' THEN COALESCE(a.end_date,'2050-01-01') ELSE b.end_date END AS end_date,
       a.lead_gen_fee,
       a.addi_shop_active,
       CASE WHEN a.addi_shop_active = true 
              AND (LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS NULL 
              OR  LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS false
              OR  LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS true)
              THEN 'opt-in'
            WHEN a.addi_shop_active = false 
              AND LAG(a.addi_shop_active) OVER (PARTITION BY a.ally_slug ORDER BY a.start_date) IS true 
              THEN 'opt-out'
            ELSE null
       END flag_active
FROM filter_table a
LEFT JOIN silver.d_marketplace_lead_gen_fee_allies_co_legacy b ON b.ally_name = a.ally_slug AND b.start_date < '2023-03-02'
WHERE addi_shop_flag = 1
UNION ALL
SELECT ally_name AS ally_slug,
       start_date,
       end_date,
       lead_gen_fee,
       NULL AS addi_shop_active,
       'opt-in' AS flag_active
FROM silver.d_marketplace_lead_gen_fee_allies_co_legacy
WHERE end_date < '2023-03-02' 
)
, status_table AS (
SELECT ally_slug,
       start_date,
       CASE WHEN end_date >= '2023-03-02' THEN COALESCE(LEAD(start_date) OVER(PARTITION BY ally_slug ORDER BY start_date), '2050-01-01') 
            ELSE end_date
       END AS end_date,
       lead_gen_fee,
       flag_active,
       CURRENT_DATE() AS ingested_at
FROM flag_table
)
SELECT DISTINCT ally_slug,
       start_date,
       end_date, 
       lead_gen_fee,
       CASE WHEN CURRENT_DATE() < end_date OR end_date IS NULL THEN 'Active' ELSE 'Inactive' END AS current_status,
       ingested_at
FROM status_table
WHERE 1=1 
AND flag_active = 'opt-in'
AND ally_slug NOT IN ('addi', 'adelante')
AND start_date < end_date