{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- QUERY

WITH base AS (
  SELECT
    ally_slug,
    category_slug,
    subCategory
  FROM (
    SELECT
      ally_slug,
      split_outer.category_substring
    FROM (
      SELECT
        ally_slug,
        split(categories_v2, '}, {') AS split_inner
      FROM {{ ref('ally_management_allies_co') }}
    )
    LATERAL VIEW OUTER explode(split_inner) split_outer AS category_substring
  ) lateral_view_outer
  LATERAL VIEW OUTER explode(split(regexp_replace(category_substring, '(\\[|\\]|\\{|\\}|")', ''), ',')) split_outer AS category_slug
  LATERAL VIEW OUTER explode(split(substring(category_substring, instr(category_substring, 'subCategories') + 17, length(category_substring) - instr(category_substring, 'subCategories') - 17), '", "')) subCategories_view AS subCategory
)
, ally_cat_subcat AS (
  SELECT
    ally_slug,
    replace(b.category_slug, 'slug: ', '') AS category_slug,
    replace(replace(b.subCategory,'"',''),']}','') AS subcategory_slug
  FROM base b
  WHERE 1=1
    AND b.category_slug ILIKE '%slug%'
)
SELECT
  ac.ally_slug,
  ac.category_slug,
  cat.category_name,
  ac.subcategory_slug,
  subcat.sub_category_name
FROM ally_cat_subcat ac
LEFT JOIN (SELECT distinct category_slug, category_name FROM  {{ ref('d_ally_management_sub_categories_co') }}) AS cat
  ON ac.category_slug = cat.category_slug
LEFT JOIN  {{ ref('d_ally_management_sub_categories_co') }} subcat
  ON ac.subcategory_slug = subcat.sub_category_slug
