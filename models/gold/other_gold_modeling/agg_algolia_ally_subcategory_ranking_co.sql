{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{%- set days_before = 30 -%}


WITH base AS (
  SELECT distinct
    a.client_id,
    a.ally_slug,
    a.subcategory AS subcategory_name,
    CASE WHEN is_origination_flag IS TRUE THEN a.application_id ELSE NULL END AS origination_application_id,
    a.shopping_intent_id,
    a.gmv,
    a.lead_gen_fee_amount
  FROM {{ ref('dm_shopping_intent_application_origination_co') }} a
  WHERE 1=1
    AND subcategory IS NOT NULL
    AND a.shopping_intent_timestamp_local >= date_add(current_date(),-{{days_before}})
) 
, metrics AS (
  SELECT
    b.ally_slug,
    am.category_slug,
    am.category_name,
    am.sub_category_slug,
    am.sub_category_name,
    subcategory_name,
    COUNT(distinct shopping_intent_id) AS clicks,
    COUNT(distinct client_id) AS users_clicking,
    COUNT(distinct origination_application_id) AS loans,
    COUNT(distinct client_id) FILTER (WHERE origination_application_id IS NOT NULL) AS users_buying,
    SUM(gmv) AS addishop_gmv,
    SUM(lead_gen_fee_amount) AS addishop_revenue
  FROM base b
  LEFT JOIN {{ ref('d_ally_management_sub_categories_co') }} am
    ON TRIM(replace(lower(TRANSLATE(b.subcategory_name,'áéíóúñ','aeioun')),'ñ','n')) = TRIM(replace(replace(lower(TRANSLATE(am.sub_category_name,'áéíóú','aeiou')),'ñ','n'),'õ','o'))
  GROUP BY ALL
) 
, ranks AS (
  SELECT *, 
    row_number() OVER (PARTITION BY sub_category_name ORDER BY loans DESC) AS loans_rank,
    row_number() OVER (PARTITION BY category_name, ally_slug ORDER BY loans DESC) AS loan_slug_subcategory_rank
	--Metrics not being used
		--row_number() OVER (PARTITION BY sub_category_name ORDER BY clicks DESC) AS clicks_subcat_rank,
		--row_number() OVER (PARTITION BY sub_category_name ORDER BY users_clicking DESC) AS users_clicking_rank,
		--row_number() OVER (PARTITION BY sub_category_name ORDER BY users_buying DESC) AS users_buying_rank,
		--row_number() OVER (PARTITION BY sub_category_name ORDER BY addishop_gmv DESC) AS addishop_gmv_rank,
		--row_number() OVER (PARTITION BY sub_category_name ORDER BY addishop_revenue DESC) AS addishop_revenue_rank,

  FROM metrics
)
, rank_category AS (
	SELECT
	ally_slug,
	category_slug,
	sub_category_slug,
	subcategory_name,
	loans_rank, 
	loan_slug_subcategory_rank, 
	CASE WHEN loans_rank = 1 AND loan_slug_subcategory_rank = 1 THEN 1 -- Hold position
		WHEN loans_rank = 1 AND loan_slug_subcategory_rank > 1 THEN 3 -- Move down
		WHEN loans_rank <= 10 AND loan_slug_subcategory_rank > 1 THEN 3 -- Move down
		WHEN loans_rank <= 10 AND loan_slug_subcategory_rank = 1 THEN 1 -- Hold position
		WHEN  loans_rank > 10 AND loan_slug_subcategory_rank > 1 THEN 2 -- Boost position
		WHEN  loans_rank > 10 AND loan_slug_subcategory_rank = 1 THEN 1 -- Hold position'
	END AS category
	FROM ranks 
	WHERE 1=1
) 
, rank_adjusted AS (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY sub_category_slug ORDER BY loans_rank) AS rank_adjusted
	FROM rank_category
	WHERE category < 3

	UNION ALL

	SELECT *,
		5 AS rank_adjusted
	FROM rank_category
	WHERE category = 3

) 
, new_rank AS (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY sub_category_slug ORDER BY rank_adjusted, category ASC, ally_slug) AS new_rank
	FROM rank_adjusted
)
--We need to avoid to shpw the same ally in the same position across subcategories that belong to the same category
, new_rank_adjusted AS (
	SELECT 	*,
		CASE WHEN (LAG(new_rank) OVER (PARTITION BY ally_slug, category_slug ORDER BY new_rank, loan_slug_subcategory_rank  )) = new_rank THEN new_rank + loan_slug_subcategory_rank - 1 ELSE new_rank END AS new_rank_adjusted
	FROM new_rank
)
SELECT
  	ally_slug,
  	category_slug,
  	sub_category_slug,
  	new_rank_adjusted AS score_a,
   	NOW() AS ingested_at,
	to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM new_rank_adjusted
WHERE 1=1
  AND category_slug IS NOT NULL
