{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH hubspot as (
SELECT ally_slug_1,
CASE WHEN pipeline_label='Brazil Sales' THEN 'BR' ELSE 'CO' END as country_code,
ally_cluster,
ally_brand,
CASE WHEN pipeline_label='Brazil Sales' THEN close_date - interval '3 hour' 
ELSE close_date - interval '5 hour' 
END as close_date,
updated_at,
row_number() OVER (PARTITION BY ally_slug_1 ORDER BY updated_at DESC) as rank
FROM  {{ ref('f_hubspot_deals_details_log') }} h
WHERE 1=1
    AND ((pipeline_label  = 'Colombia Sales' AND upper(h.stage_label) in ('ACTIVE', 'ACTIVATION', 'ON HOLD','ONBOARDING','LOST'))
    OR (pipeline_label  = 'Brazil Sales'))
QUALIFY rank=1
),

salesforce as (
SELECT opportunity_slug,
'CO' as country_code,
opportunity_ally_cluster,
opportunity_close_date::timestamp,
account_kam_name,
account_kam_email,
account_pod,
opportunity_amount,
opportunity_account_vertical,
row_number() over (partition by opportunity_slug order by opportunity_systemmodstamp desc) as slug_order
FROM {{ ref('d_salesforce_opportunities') }}
WHERE 1=1
AND opportunity_slug IS NOT NULL AND opportunity_record_type_name != 'Addishop' AND opportunity_is_deleted IS NOT True
QUALIFY slug_order = 1
),

base_br AS (
SELECT
  "BR" as country_code,
	al.ally_slug,
	COALESCE(al.brand_slug,h.ally_brand) AS ally_brand,
	al.vertical_slug AS ally_vertical,
	CASE WHEN NULLIF(TRIM(ally_cluster),'') is not null and ally_cluster<>'Corporate' then ally_cluster
  ELSE "SMB" END as ally_cluster,
  h.close_date::date,
  sfa.slug_first_app_date,
  null as account_kam_name,
  null as account_kam_email,
  null as account_pod,
  null as opportunity_amount, 
  NAMED_STRUCT('ally_cluster',NAMED_STRUCT('salesforce',null,
                                           'hubspot', h.ally_cluster),
               'close_date',NAMED_STRUCT('salesforce',null,
                                           'hubspot',h.close_date),
               'ally_vertical',NAMED_STRUCT('salesforce',null,
                                           'ally_management',al.vertical_slug),
               'ally_brand',NAMED_STRUCT('ally_management',al.brand_name,
                                           'hubspot',h.close_date)
               ) as debug_data
FROM {{ ref('d_ally_management_allies_br') }} al 
LEFT JOIN hubspot h
	ON h.ally_slug_1 = al.ally_slug and h.country_code='BR'
LEFT JOIN (select 
                distinct(ally_slug) as ally_slug
                ,min(application_date -interval '3 hour')  as slug_first_app_date
                from {{ ref('f_applications_br') }} a
                group by 1
                ) sfa on al.ally_slug=sfa.ally_slug
WHERE 1=1
),

base_co AS (
  SELECT
  "CO" as country_code,
	al.ally_slug,
	COALESCE(al.brand_name,h.ally_brand) AS ally_brand,
	COALESCE(s.opportunity_account_vertical,al.vertical_slug) AS ally_vertical,
	CASE WHEN al.ally_slug = 'addi-marketplace' THEN 'KA' -- temporary
	     WHEN COALESCE(s.opportunity_ally_cluster,h.ally_cluster)='Corporate' THEN 'KA'
	     WHEN COALESCE(s.opportunity_ally_cluster,h.ally_cluster)='SMBs' THEN 'SMB'
  ELSE COALESCE(s.opportunity_ally_cluster,h.ally_cluster,"SMB")
  END AS ally_cluster,
  COALESCE(s.opportunity_close_date,h.close_date)::date close_date,
  sfa.slug_first_app_date,
  account_kam_name,
  account_kam_email,
  account_pod,
opportunity_amount,
  NAMED_STRUCT('ally_cluster',NAMED_STRUCT('salesforce',s.opportunity_ally_cluster,
                                           'hubspot', h.ally_cluster),
               'close_date',NAMED_STRUCT('salesforce',s.opportunity_close_date,
                                           'hubspot',h.close_date),
               'ally_vertical',NAMED_STRUCT('salesforce',s.opportunity_account_vertical,
                                           'ally_management',al.vertical_slug),
               'ally_brand',NAMED_STRUCT('ally_management',al.brand_name,
                                           'hubspot',h.close_date)
               ) as debug_data
FROM {{ ref('d_ally_management_allies_co') }} al 
LEFT JOIN salesforce s
  ON s.opportunity_slug=al.ally_slug
LEFT JOIN hubspot h
	ON h.ally_slug_1 = al.ally_slug and h.country_code='CO'
LEFT JOIN (select 
                distinct(ally_slug) as ally_slug
                ,min(application_date - interval '5 hour')  as slug_first_app_date
                from {{ source('silver_live', 'f_applications_co') }} a
                group by 1
                ) sfa on al.ally_slug=sfa.ally_slug
WHERE 1=1
),

final_table AS (
  SELECT * FROM base_br
  UNION ALL
  SELECT * FROM base_co)

  SELECT *,
  CASE WHEN date_trunc('quarter',current_date()) < min(close_date) over (partition by ally_brand) THEN 'New'
  ELSE 'Existing' END AS status_ally_brand,
  CASE WHEN close_date > date_trunc('quarter',current_date()) THEN 'New'
  WHEN (COALESCE(close_date)) <= date_trunc('quarter',current_date())
  THEN 'Existing'
  ELSE 'Not Available' END AS status_ally_slug 
FROM final_table
