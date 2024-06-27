



WITH loan_data AS (
    SELECT 
        client_id,
        loan_id,
        approved_amount,
        from_utc_timestamp(origination_date, "America/Bogota") AS origination_date,
        state,
        first_value(from_utc_timestamp(origination_date, "America/Bogota")) OVER (PARTITION BY client_id ORDER BY origination_date) AS first_origination_date,
        first_value(from_utc_timestamp(origination_date, "America/Bogota")) OVER (PARTITION BY client_id ORDER BY origination_date DESC) AS last_origination_date,
        row_number() OVER (PARTITION BY client_id ORDER BY origination_date) AS rn,
        row_number() OVER (PARTITION BY client_id ORDER BY origination_date desc) AS rn_desc
    FROM gold.dm_loan_status_co
),
preap_apps AS (
    SELECT 
        *,
        prospect_id AS client_id
    FROM gold.rmt_application_preapproval_co
    WHERE rn = 1
),
first_loan_data AS (
    SELECT 
        client_id,
        approved_amount,
        origination_date AS first_origination_date
    FROM loan_data
    WHERE rn = 1
),
vertical AS (
	SELECT
		DISTINCT ally_slug,
		ally_vertical:name.value AS vertical
	FROM silver.d_ally_management_stores_allies_co
),
app_count AS (
    SELECT 
        a.client_id,
        orig.loan_id,
        a.application_id,
        from_utc_timestamp(a.application_date, "America/Bogota") AS application_date,
        a.ally_slug as ally_name,
        a.requested_amount,
        v.vertical,
        fld.first_origination_date,
        CASE
            WHEN from_utc_timestamp(a.application_date, "America/Bogota") < fld.first_origination_date OR fld.first_origination_date IS NULL THEN 'PROSPECT' ELSE 'CLIENT'
        END AS client_type
    FROM silver.f_applications_co a
    left join silver.f_originations_bnpl_co orig
      on a.application_id = orig.application_id
    LEFT JOIN first_loan_data fld 
        ON a.client_id = fld.client_id
    LEFT JOIN vertical v
        ON a.ally_slug = v.ally_slug
    WHERE a.ally_slug != 'addi-preapprovals'
        AND (a.journey_name IS NULL OR a.journey_name NOT LIKE '%preap%')
),
raw_behaviour AS (
    SELECT 
        ac.client_id,
        max(pa.preapproval_amount) AS preapp_amount,
        sum(CASE WHEN ac.application_date - INTERVAL '7 days' < pa.application_date THEN ac.requested_amount END) AS apps7days,
        sum(CASE WHEN ac.application_date - INTERVAL '7 days' < pa.application_date THEN ac.requested_amount END) / max(pa.preapproval_amount) AS p_apps7days
    FROM app_count ac
    LEFT JOIN preap_apps pa 
        ON pa.client_id = ac.client_id
    GROUP BY 1
),
raw_behaviour_loan AS (
    SELECT 
        ld.client_id,
        sum(CASE WHEN ld.rn = 2 AND ld.origination_date - INTERVAL '7 days' < ld.first_origination_date THEN ac.requested_amount END) / sum(CASE WHEN ld.rn = 1 THEN ac.requested_amount END) AS second_vs_first,
        sum(CASE WHEN ld.rn_desc = 2 AND ld.origination_date - INTERVAL '7 days' < ld.first_origination_date THEN ac.requested_amount END) / sum(CASE WHEN ld.rn_desc = 1 THEN ac.requested_amount END) AS penu_vs_latest
    FROM loan_data ld
    LEFT JOIN app_count ac 
        ON ld.loan_id = ac.loan_id
    GROUP BY 1
),
apps_7_days AS (
    SELECT 
        t1.application_id,
        t1.loan_id,
        t1.client_id,
        count(t2.application_id) AS weekly_apps,
        sum(CASE WHEN t1.application_id IS NOT NULL AND t1.requested_amount > t2.requested_amount THEN 1 END) AS lesser_weekly_apps,
        sum(CASE WHEN t1.ally_name != t2.ally_name AND t1.ally_name IS NOT NULL AND t2.ally_name IS NOT NULL AND t1.requested_amount != t2.requested_amount THEN 1 END) AS diff_ally_n_amount_weekly,
        sum(CASE WHEN t1.ally_name != t2.ally_name AND t1.ally_name IS NOT NULL AND t2.ally_name IS NOT NULL AND t1.requested_amount < t2.requested_amount THEN 1 END) AS diff_ally_less_amount_weekly,
        sum(CASE WHEN t1.requested_amount < t2.requested_amount THEN 1 END) AS less_amount_weekly,
        sum(CASE WHEN t1.vertical != t2.vertical THEN 1 END) AS diff_vertical_weekly
    FROM app_count t1
    LEFT JOIN app_count t2 
        ON t1.client_id = t2.client_id
        AND t2.application_date BETWEEN t1.application_date - interval '7 days' AND t1.application_date
        AND t1.application_id != t2.application_id
    WHERE t1.client_type = 'PROSPECT'
    GROUP BY 
        t1.application_id,
        t1.loan_id,
        t1.client_id
),
apps_30_days AS (
    SELECT 
        t1.application_id,
        t1.loan_id,
        t1.client_id,
        count(t2.application_id) AS monthly_apps,
        sum(CASE WHEN t1.application_id IS NOT NULL AND t1.requested_amount > t2.requested_amount THEN 1 END) AS lesser_monthly_apps,
        sum(CASE WHEN t1.ally_name != t2.ally_name AND t1.ally_name IS NOT NULL AND t2.ally_name IS NOT NULL AND t1.requested_amount != t2.requested_amount THEN 1 END) AS diff_ally_n_amount_monthly,
        sum(CASE WHEN t1.ally_name != t2.ally_name AND t1.ally_name IS NOT NULL AND t2.ally_name IS NOT NULL AND t1.requested_amount < t2.requested_amount THEN 1 END) AS diff_ally_less_amount_monthly,
        sum(CASE WHEN t1.requested_amount < t2.requested_amount THEN 1 END) AS less_amount_monthly,
        sum(CASE WHEN t1.vertical != t2.vertical THEN 1 END) AS diff_vertical_monthly
    FROM app_count t1
    LEFT JOIN app_count t2 
        ON t1.client_id = t2.client_id
        AND t2.application_date BETWEEN t1.application_date - interval '30 days' AND t1.application_date
        AND t1.application_id != t2.application_id
    WHERE t1.client_type = 'PROSPECT'
    GROUP BY 
        t1.application_id,
        t1.loan_id,
        t1.client_id
)
SELECT 
    apps_7_days.application_id,
    apps_7_days.loan_id,
    apps_7_days.client_id,
    apps_7_days.weekly_apps,
    apps_7_days.lesser_weekly_apps,
    apps_7_days.diff_ally_n_amount_weekly,
    apps_7_days.diff_ally_less_amount_weekly,
    apps_7_days.less_amount_weekly,
    apps_7_days.diff_vertical_weekly,
    apps_30_days.monthly_apps,
    apps_30_days.lesser_monthly_apps,
    apps_30_days.diff_ally_n_amount_monthly,
    apps_30_days.diff_ally_less_amount_monthly,
    apps_30_days.less_amount_monthly,
    apps_30_days.diff_vertical_monthly,
    raw_behaviour.apps7days,
    raw_behaviour.p_apps7days,
    raw_behaviour.preapp_amount,
    raw_behaviour_loan.second_vs_first
FROM apps_7_days
LEFT JOIN apps_30_days 
    ON apps_7_days.application_id = apps_30_days.application_id
LEFT JOIN raw_behaviour 
    ON apps_7_days.client_id = raw_behaviour.client_id
LEFT JOIN raw_behaviour_loan 
    ON apps_7_days.client_id = raw_behaviour_loan.client_id