{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with co_originations AS (
    SELECT 
    'CO' as country,
    3900.00 AS xrate,
    apps.application_id,
    orig.loan_id,
    COALESCE(apps.channel, apps.application_channel_legacy, 'UNKNOWN') AS channel,
    apps.product,
    (CASE WHEN apps.requested_amount_without_discount IS NOT NULL AND apps.requested_amount_without_discount > 0 THEN apps.requested_amount_without_discount ELSE apps.requested_amount END) AS gmv_local,
    lp.ally_mdf,
    from_utc_timestamp(orig.origination_date, 'America/Bogota') AS origination_datetime_local
    FROM {{ source('silver_live', 'f_applications_co') }} AS apps
    INNER JOIN {{ source('silver_live', 'f_originations_bnpl_co') }} AS orig 					ON apps.application_id = orig.application_id
    LEFT JOIN {{ source('silver_live', 'f_loan_proposals_co') }} AS lp 						ON orig.loan_id = lp.loan_proposal_id
    WHERE from_utc_timestamp(apps.application_date, 'America/Bogota')::date < from_utc_timestamp(now(), 'America/Bogota')::date   
)
,
br_originations AS (
    SELECT 
    'BR' as country,
    5.59 AS xrate,
    apps.application_id,
    orig.loan_id,
    COALESCE(apps.channel, apps.application_channel_legacy, 'UNKNOWN') AS channel,
    apps.product,
    (CASE WHEN apps.requested_amount_without_discount IS NOT NULL AND apps.requested_amount_without_discount > 0 THEN apps.requested_amount_without_discount ELSE apps.requested_amount END) AS gmv_local,
    lp.ally_mdf,
    from_utc_timestamp(orig.origination_date, 'America/Sao_Paulo') AS origination_datetime_local
    FROM {{ ref('f_applications_br') }} AS apps
    INNER JOIN {{ ref('f_originations_bnpl_br') }} AS orig 					ON apps.application_id = orig.application_id
    LEFT JOIN {{ ref('f_loan_proposals_br') }} AS lp 						ON orig.loan_id = lp.loan_proposal_id
    WHERE from_utc_timestamp(apps.application_date, 'America/Sao_Paulo')::date < from_utc_timestamp(now(), 'America/Sao_Paulo')::date
)
,
originations AS (
    (SELECT * FROM co_originations WHERE channel != 'PRE_APPROVAL')
    UNION ALL
    (SELECT * FROM br_originations)
)
,
final_table AS (
	SELECT
        country,
		DATE_TRUNC('month', origination_datetime_local)::date AS period,
        product,
		SUM(gmv_local/xrate) AS gmv,
		SUM(ally_mdf * gmv_local)/ SUM(gmv_local) as weighted_mdf,
		COUNT(1) AS num_records
	FROM originations
	GROUP BY 1,2,3
	ORDER BY 1,2,3
)
SELECT * 
FROM final_table
