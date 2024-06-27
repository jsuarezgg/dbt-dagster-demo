

WITH

dm_originations_co AS (
    SELECT * FROM gold.dm_originations WHERE country_code = 'CO'
),

d_ally_management_allies_co AS (
    SELECT ally_slug, vertical_name as vertical FROM silver.d_ally_management_allies_co
),

f_applications_co AS (
    SELECT * FROM silver.f_applications_co
    WHERE cast(application_date AS date) <= "2022-01-01"::date
),

d_syc_loan_status_co AS (
    SELECT * FROM silver.d_syc_loan_status_co
),

f_syc_clients_addicupo_history_co AS (
    SELECT * FROM silver.f_syc_clients_addicupo_history_co
),

f_kyc_bureau_income_estimator_co AS (
    SELECT * FROM silver.f_kyc_bureau_income_estimator_co
),

dm_daily_loan_status_co AS (
    SELECT * FROM gold.dm_daily_loan_status_co
),

f_snc_payments_report_co AS (
    SELECT * FROM silver.f_snc_payments_report_co
),

kus_messages_co AS (
    SELECT * FROM cur.kus_messages
),

crm_clients AS (
    SELECT * FROM cur.crm_clients
),

dm_amplitude AS (
    SELECT * FROM gold.dm_amplitude
),

applications_data AS (

    SELECT
        apps.client_id,
        COUNT(*) AS n_applications,
        COUNT(
            CASE
                WHEN apps.application_date::date > "2022-01-01"::date - interval '1 week' THEN 1
                ELSE NULL
            END
            ) AS n_applications_last_week,
        COUNT(
            CASE
                WHEN apps.application_date::date > "2022-01-01"::date - interval '1 month' THEN 1
                ELSE NULL
            END
            ) AS n_applications_last_month,
        MAX(apps.application_date) AS last_application_date,
        COUNT(*) as n_slugs,
        COUNT(DISTINCT apps.ally_slug) as n_different_slugs,
        COUNT(DISTINCT av.vertical) as n_different_verticals
    FROM f_applications_co apps
    LEFT JOIN dm_originations_co orig ON apps.application_id = orig.application_id
    LEFT JOIN d_ally_management_allies_co av ON apps.ally_slug = av.ally_slug
    GROUP BY apps.client_id
),

applications_ordered AS (
    SELECT
        apps.client_id AS client_id,
        application_date,
        CASE WHEN orig.application_id IS NOT NULL THEN TRUE ELSE FALSE END AS loan_accepted,
        orig.approved_amount,
        apps.ally_slug,
        av.vertical,
        row_number() OVER (PARTITION BY apps.client_id
            ORDER BY application_date DESC
        ) AS application_last,
        row_number() OVER (PARTITION BY apps.client_id
            ORDER BY application_date ASC
        ) AS application_first
    FROM f_applications_co apps
    LEFT JOIN dm_originations_co orig on apps.application_id = orig.application_id
    LEFT JOIN d_ally_management_allies_co av on apps.ally_slug = av.ally_slug
    ORDER BY
        client_id,
        application_date DESC
),

originations_ordered AS (
    SELECT
        apps.client_id,
        apps.application_date,
        true as loan_accepted,
        orig.approved_amount,
        apps.ally_slug,
        av.vertical,
        row_number() OVER (PARTITION BY apps.client_id
            ORDER BY application_date DESC
        ) AS origination_last,
        row_number() over (PARTITION BY apps.client_id
            ORDER BY application_date ASC
        ) AS origination_first,
        orig.origination_date AS origination_date
    FROM f_applications_co apps
    INNER JOIN dm_originations_co orig ON apps.application_id = orig.application_id
    LEFT JOIN d_ally_management_allies_co av ON apps.ally_slug = av.ally_slug
    WHERE orig.origination_date::date <= "2022-01-01"
    ORDER BY
        client_id,
        application_date DESC
),

last_application AS (
    SELECT
        *
    FROM applications_ordered
    WHERE application_last = 1
),

first_application AS (
    SELECT
        *,
        datediff("2022-01-01"::date, application_date::date) as n_active_days
    FROM applications_ordered
    WHERE application_first = 1
),

last_origination AS (
    SELECT
        *
    FROM originations_ordered
    WHERE origination_last = 1
),

first_origination AS (
    SELECT
        *
    FROM originations_ordered
    WHERE origination_first = 1
),

applications_timed AS (
    SELECT
        la.client_id,
        la.ally_slug AS last_slug,
        la.vertical AS last_vertical,
        fa.ally_slug AS first_slug,
        fa.vertical AS first_vertical,
        fa.n_active_days,
        fa.application_date AS first_origination_date,
        lo.application_date AS last_origination_date
    FROM last_application la FULL
    OUTER JOIN first_application fa ON la.client_id = fa.client_id FULL
    OUTER JOIN last_origination lo ON la.client_id = lo.client_id
),

loan_status as (
    SELECT
        client_id,
        count(distinct loan_id) as n_active_loans
    FROM dm_daily_loan_status_co
    WHERE calculation_date = "2022-01-01"
    GROUP BY 1
),

addicupo_all_updates_co AS (
    SELECT
        client_id,
        remaining_addicupo,
        COALESCE(addicupo_state_v2, addicupo_state) AS addicupo_state,
        addicupo_last_update,
        row_number() OVER (PARTITION BY client_id ORDER BY addicupo_last_update DESC) AS n_update
    FROM f_syc_clients_addicupo_history_co
    WHERE addicupo_last_update::date <= "2022-01-01"
),

addi_cupo_updates_co AS (
    SELECT
        client_id,
        remaining_addicupo,
        addicupo_state,
        addicupo_last_update
    FROM addicupo_all_updates_co
    WHERE n_update = 1
),

incomes AS (
    SELECT
        client_id,
        collect_list(income_estimatedIncome) as estimated_income
    FROM silver.f_kyc_bureau_income_estimator_co
    WHERE ocurred_on_date::date <= "2022-01-01"
    GROUP BY 1
),

client_loan_status_features AS (
    SELECT
      client_id
      ,min(calculation_date)filter(where paid_installments > 0) as first_installment_paid_date
      ,min(calculation_date)filter(where is_fully_paid is true) as first_fully_paid_loan_date
      ,max(days_past_due) as max_days_past_due
    FROM dm_daily_loan_status_co
    WHERE calculation_date::date <= "2022-01-01"
    GROUP BY 1
),

installments AS (
    SELECT client_id
           ,count(*) FILTER(WHERE delinquency_iof != 0
                        OR principal_overdue != 0
                        OR moratory_interest != 0
                        OR interest_overdue != 0
                        OR guarantee_overdue != 0) as installment_paid_in_delinquency_n
           ,count(*) FILTER(WHERE delinquency_iof != 0
                        OR principal_overdue != 0
                        OR moratory_interest != 0
                        OR interest_overdue != 0
                        OR guarantee_overdue != 0) / count(*) * 100 as installment_paid_in_delinquency_proportion
    FROM f_snc_payments_report_co
    WHERE date::date <= "2022-01-01"
    GROUP BY 1
),

contacts AS (
    SELECT
        customerId,
        client_id,
        min(sentAt) as first_contact
    FROM kus_messages_co km
    INNER JOIN crm_clients cc
    ON km.customerId = cc.kustomer_id
    GROUP BY 1, 2
),

client_purchase_metrics_co AS (
    SELECT
      client_id,
      loan_id,
      origination_date,
      processed_product,
      approved_amount,
      synthetic_channel,
      row_number() over (partition by client_id order by origination_date) AS loan_number,
      lag(origination_date) over (partition by client_id order by origination_date) as prev_purchase,
      COALESCE(datediff(origination_date,lag(origination_date) over (partition by client_id order by origination_date)),0) as prev_purchase_datediff
    FROM dm_originations_co
    WHERE origination_date::date <= "2022-01-01"
),

client_purchase_metrics_agg_co AS (
    SELECT
        client_id,
        COUNT(distinct loan_id) AS n_originations,
        COUNT(distinct loan_id) FILTER (WHERE origination_date::date > "2022-01-01"::date - interval '1 week') AS n_originations_last_week,
        COUNT(distinct loan_id) FILTER (WHERE origination_date::date > "2022-01-01"::date - interval '1 month') AS n_originations_last_month,
        COUNT(distinct loan_id) FILTER (WHERE processed_product = 'PAGO_CO') AS n_pago_originated_loans,
        SUM(approved_amount) AS total_GMV_originated,
        AVG(approved_amount) as avg_ammount_loan,
        MAX(approved_amount) as max_ammount_loan,
        COUNT(*) FILTER(WHERE synthetic_channel ILIKE "%COMM%") AS n_eccom_originated_loans,
        COUNT(*) FILTER(WHERE synthetic_channel NOT ILIKE "%COMM%") AS n_physical_originated_loans,
        COUNT(distinct loan_id) FILTER (WHERE origination_date::date >= date_add("2022-01-01",-30)) AS originations_last_30_days,
        COUNT(distinct loan_id) FILTER (WHERE origination_date::date >= date_add("2022-01-01",-90)) AS originations_last_90_days,
        COUNT(distinct loan_id) FILTER (WHERE origination_date::date >= date_add("2022-01-01",-180) 
                                                AND origination_date::date < date_add("2022-01-01",-90)) AS originations_last_90_180_days,
        COUNT(distinct loan_id) FILTER (WHERE origination_date::date < date_add("2022-01-01",-180)) AS originations_more_180_days_ago,
        AVG(prev_purchase_datediff) FILTER (WHERE loan_number > 1 ) AS avg_time_next_loan
    FROM client_purchase_metrics_co
    GROUP BY 1
),

app_metrics AS (
    SELECT 
        user_id,
        event_time,
        session_id
    FROM dm_amplitude
    WHERE 1=1
      AND lower(event_type) = 'app_screen_opened'
      AND lower(source) = 'mobile_app'
      AND lower(screen_name) = 'home'
      AND event_date::date <= "2022-01-01"
),

app_metrics_final AS (

    SELECT
        user_id,
        COUNT(distinct session_id) FILTER (WHERE date_trunc('month',event_time) = date_trunc('month',"2022-01-01")) AS app_sessions_in_calculation_month,
        COUNT(distinct session_id) FILTER (WHERE event_time::date >= date_add("2022-01-01",-30)) AS app_sessions_last_30_days,
        COUNT(distinct date_Trunc('day', event_time)) FILTER (WHERE date_Trunc('day', event_time) >= date_add("2022-01-01",-30)) AS app_days_opened_last_30_days,
        COUNT(distinct session_id) AS app_sessions,
        COUNT(distinct date_Trunc('day', event_time)) AS app_days_opened
    FROM app_metrics
    GROUP BY user_id

),

final_table_co AS (
    SELECT
        "2022-01-01" AS calculation_date,
        'CO' AS country,
        ad.client_id,
        ad.n_applications,
        ad.n_applications_last_week,
        ad.n_applications_last_month,
        ad.last_application_date,
        ad.n_slugs,
        ad.n_different_slugs,
        ad.n_different_verticals,
        i.estimated_income,
        lo.approved_amount AS last_ammount_loan,
        ati.last_slug,
        ati.last_vertical,
        ati.first_slug,
        ati.first_vertical,
        fo.origination_date AS first_origination_date,
        lo.origination_date AS last_origination_date,
        ati.n_active_days,
        COALESCE(ls.n_active_loans, 0) AS n_active_loans,
        sc.remaining_addicupo,
        sc.addicupo_state,
        sc.addicupo_last_update,
        cpd.first_installment_paid_date,
        cpd.first_fully_paid_loan_date,
        cpd.max_days_past_due,
        ins.installment_paid_in_delinquency_n,
        ins.installment_paid_in_delinquency_proportion,
        con.first_contact,
        COALESCE(cpm.n_originations, 0) AS n_originations,
        COALESCE(cpm.n_originations_last_week, 0) AS n_originations_last_week,
        COALESCE(cpm.n_originations_last_month, 0) AS n_originations_last_month,
        COALESCE(cpm.n_pago_originated_loans, 0) AS n_pago_originated_loans,
        COALESCE(cpm.total_GMV_originated, 0) AS total_GMV_originated,
        COALESCE(cpm.avg_ammount_loan, 0) AS avg_ammount_loan,
        COALESCE(cpm.max_ammount_loan, 0) AS max_ammount_loan,
        COALESCE(cpm.n_eccom_originated_loans, 0) AS n_eccom_originated_loans,
        COALESCE(cpm.n_physical_originated_loans, 0) AS n_physical_originated_loans,
        COALESCE(cpm.originations_last_30_days, 0) AS originations_last_30_days,
        COALESCE(cpm.originations_last_90_days, 0) AS originations_last_90_days,
        COALESCE(cpm.originations_last_90_180_days, 0) AS originations_last_90_180_days,
        COALESCE(cpm.originations_more_180_days_ago, 0) AS originations_more_180_days_ago,
        cpm.avg_time_next_loan,
        COALESCE(am.app_sessions_in_calculation_month, 0) AS app_sessions_in_calculation_month,
        COALESCE(am.app_sessions_last_30_days, 0) AS app_sessions_last_30_days,
        COALESCE(am.app_days_opened_last_30_days, 0) AS app_days_opened_last_30_days,
        COALESCE(am.app_sessions, 0) AS app_sessions,
        COALESCE(am.app_days_opened, 0) AS app_days_opened,
        CASE
            WHEN cpm.originations_last_90_days > 0 AND cpm.originations_last_90_180_days = 0 AND cpm.originations_more_180_days_ago = 0 THEN 'New'
            WHEN cpm.originations_last_90_days = 0  AND cpm.originations_last_90_180_days > 0 AND cpm.originations_more_180_days_ago = 0 THEN 'Churned'
            WHEN cpm.originations_last_90_days = 0  AND cpm.originations_last_90_180_days > 0 AND cpm.originations_more_180_days_ago > 0 THEN 'Churned'
            WHEN cpm.originations_last_90_days = 0  AND cpm.originations_last_90_180_days = 0 AND cpm.originations_more_180_days_ago > 0 THEN 'Lost'
            WHEN cpm.originations_last_90_days > 0  AND cpm.originations_last_90_180_days > 0 AND cpm.originations_more_180_days_ago = 0 THEN 'Retained'
            WHEN cpm.originations_last_90_days > 0  AND cpm.originations_last_90_180_days > 0 AND cpm.originations_more_180_days_ago > 0 THEN 'Retained'
            WHEN cpm.originations_last_90_days > 0  AND cpm.originations_last_90_180_days = 0 AND cpm.originations_more_180_days_ago > 0 THEN 'Resurrected'
            WHEN (cpm.n_originations = 0 OR cpm.n_originations IS NULL) THEN 'Not a Client'
        END AS client_category
    FROM
        applications_data ad
        left join applications_timed ati on ad.client_id = ati.client_id
        left join loan_status ls on ad.client_id = ls.client_id
        left join addi_cupo_updates_co sc on ad.client_id = sc.client_id
        left join incomes i on ad.client_id = i.client_id
        left join client_loan_status_features cpd on ad.client_id = cpd.client_id
        left join installments ins on ad.client_id = ins.client_id
        left join contacts con on ad.client_id = con.client_id
        left join client_purchase_metrics_agg_co cpm on ad.client_id = cpm.client_id
        left join app_metrics_final am ON am.user_id = cpm.client_id
        left join first_origination fo ON ad.client_id = fo.client_id
        left join last_origination lo ON ad.client_id = lo.client_id

)

SELECT * FROM final_table_co;