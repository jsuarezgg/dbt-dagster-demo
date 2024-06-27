

WITH syc_loan_status AS (

    SELECT * FROM silver.d_syc_loan_status_co

),
snc_payments_report AS (

    SELECT * FROM silver.f_snc_payments_report_co

),
syc_originations AS (

    SELECT * FROM silver.f_syc_originations_co

),
syc_refinancing_instructions AS (

    SELECT * FROM silver.f_syc_refinancing_instructions_co

),
client_loans AS (

    SELECT * FROM silver.f_fincore_loans_co

),
dynamic_funnel_loan_proposals AS (

    SELECT * FROM silver.f_loan_proposals_co

),
originations AS (

    SELECT * FROM silver.f_originations_bnpl_co

),
applications AS (

    SELECT * FROM silver.f_applications_co

),
allies AS (

    SELECT * FROM silver.d_ally_management_stores_allies_co

),
pr_y_df_2 AS (

    SELECT
        pr_df.index,
        pr_df.client_id,
        pr_df.delinquency_iof,
        pr_df.moratory_interest,
        pr_df.interest_overdue,
        pr_df.principal_overdue,
        pr_df.guarantee_overdue,
        pr_df.current_interest,
        pr_df.current_principal,
        pr_df.current_guarantee,
        pr_df.unpaid_guarantee,
        pr_df.unpaid_principal,
        pr_df.prepayment_benefit,
        pr_df.date,
        pr_df.payment_id,
        pr_df.ownership,
        pr_df.channel,
        pr_df.loan_id,
        pr_df.leftover,
        pr_df.late_fees,
        pr_df.total_payment,
        pr_df.created_at,
        pr_df.ingested_at,
        pr_df.updated_at,
        ls.payday,
        ls.calculation_date,
        CASE WHEN pr_df.date
            BETWEEN ls.payday - INTERVAL '1' MONTH AND ls.calculation_date
            THEN pr_df.moratory_interest + pr_df.interest_overdue + pr_df.principal_overdue +
                pr_df.current_interest + pr_df.current_principal + pr_df.unpaid_principal +
                pr_df.guarantee_overdue + pr_df.current_guarantee + pr_df.unpaid_guarantee
        END AS sum_individual_pmt,
        CASE WHEN pr_df.date
            BETWEEN ls.payday - INTERVAL '1' MONTH AND ls.calculation_date
            THEN pr_df.principal_overdue + pr_df.current_principal + pr_df.unpaid_principal
        END AS sum_individual_principal_pmt
    FROM snc_payments_report pr_df
    LEFT JOIN syc_loan_status ls ON pr_df.loan_id = ls.loan_id

),
pr_y_df AS (

    SELECT
        client_id,
        loan_id,
        total_payment AS total_pmt,
        concat_ws('|', collect_set(channel)) AS channel_str_agg,
        concat_ws('|', collect_set(ownership)) AS payments_ownership_agg,
        count(1) AS num_payments,
        sum(sum_individual_principal_pmt) AS principal_pmt,
        avg(sum_individual_principal_pmt) AS avg_pmt,
        stddev(sum_individual_principal_pmt) AS stdev_pmt
    FROM pr_y_df_2 pr_df
    GROUP BY 1, 2, 3

),
ls_df_1 AS (

    SELECT
        ls.calculation_date,
        ls.loan_id,
        ls.client_id,
        ls.accrued_interest_this_month,
        ls.applicable_rate,
        ls.condoned_interest_this_month,
        ls.current_installment_amount,
        ls.current_interest_covered_this_month,
        ls.current_interest_covered_this_period,
        ls.current_interest_due_this_period,
        ls.current_principal_covered_this_month,
        ls.current_principal_due_this_period,
        ls.days_past_due,
        ls.full_payment,
        ls.initial_installment_amount,
        ls.interest_condoned_in_first_period,
        ls.interest_on_overdue_principal,
        ls.interest_on_overdue_principal_covered_this_month,
        ls.interest_on_overdue_principal_covered_this_period,
        ls.interest_overdue,
        ls.interest_overdue_covered_this_month,
        ls.interest_overdue_covered_this_period,
        ls.ipmt,
        ls.is_fully_paid,
        ls.min_payment,
        ls.months_on_books,
        ls.paid_installments,
        ls.payday,
        ls.period_prepayment_benefit,
        ls.ppmt,
        ls.principal_overdue,
        ls.principal_overdue_covered_this_month,
        ls.principal_overdue_covered_this_period,
        ls.total_current_interest_condoned,
        ls.total_current_principal_condoned,
        ls.total_interest_overdue_condoned,
        ls.total_interest_paid,
        ls.total_moratory_interest_condoned,
        ls.total_payment_applied,
        ls.total_prepayment_benefit,
        ls.total_principal_overdue_condoned,
        ls.total_principal_paid,
        ls.total_unpaid_principal_condoned,
        ls.unpaid_interest,
        ls.unpaid_principal,
        ls.guarantee_overdue,
        ls.current_guarantee_due_this_period,
        ls.unpaid_guarantee,
        ls.total_guarantee_paid,
        ls.guarantee_covered_this_period,
        ls.guarantee_covered_this_month,
        ls.total_guarantee_condoned,
        ls.vintage,
        ls.late_fees,
        ls.total_late_fees_paid,
        ls.total_late_fees_condoned,
        ls.ingested_at,
        ls.updated_at,
        pr_y_df.channel_str_agg,
        pr_y_df.payments_ownership_agg,
        pr_y_df.num_payments,
        pr_y_df.total_pmt,
        pr_y_df.principal_pmt,
        pr_y_df.avg_pmt,
        pr_y_df.stdev_pmt,
        so.first_payment_date,
        so.origination_date,
        so.cancellation_reason
    FROM syc_loan_status ls
    LEFT JOIN pr_y_df ON ls.loan_id = pr_y_df.loan_id
    LEFT JOIN syc_originations so ON ls.loan_id = so.loan_id

),
ri_df_1 AS (

    SELECT
        client_id,
        loan_id,
        start_date AS refinancing_process_start_date,
        end_date AS refinancing_process_end_date,
        type AS refinancing_process_type,
        annulled AS refinancing_process_annulled,
        annulment_reason AS refinancing_process_annulment_reason,
        row_number() OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS refinancing_order
    FROM syc_refinancing_instructions

),
ri_df AS (

    SELECT
        *
    FROM ri_df_1
    WHERE refinancing_order = 1

),
ls_df AS (

    SELECT
        ls_df_1.*,
        ri_df.refinancing_process_start_date,
        ri_df.refinancing_process_end_date,
        ri_df.refinancing_process_type,
        ri_df.refinancing_process_annulled,
        ri_df.refinancing_process_annulment_reason AS refinancing_process_annullment_reason,
        ri_df.refinancing_order
    FROM ls_df_1
    LEFT JOIN ri_df ON ls_df_1.loan_id = ri_df.loan_id

),
lpt_df AS (

    SELECT
        cld.client_id,
        cld.loan_id,
        cld.state,
        lpt.loan_proposal_id,
        lpt.ally_mdf,
        lpt.ally_slug AS ally_name,
        lpt.interest_rate,
        lpt.lbl,
        lpt.learning_population,
        lpt.decision_npv,
        lpt.first_loan_npv,
        lpt.lifetime_npv,
        lpt.discount_rate,
        lpt.first_loan_roe,
        lpt.first_loan_cash,
        lpt.lifetime_roe,
        lpt.lifetime_cash,
        lpt.contribution_margin,
        lpt.total_fga_rate,
        lpt.fga_comission_rate,
        lpt.fga_client_rate,
        lpt.last_event_name_processed AS stage_loan_proposals_t
    FROM client_loans cld
    LEFT JOIN dynamic_funnel_loan_proposals lpt
    ON cld.loan_id = lpt.loan_proposal_id AND cld.application_id = lpt.application_id

),
loan_status_trusted_df_1 AS (

    SELECT
        ls_df.*,
        lpt_df.state,
        lpt_df.loan_proposal_id,
        lpt_df.ally_mdf,
        lpt_df.ally_name,
        lpt_df.interest_rate,
        lpt_df.lbl,
        lpt_df.learning_population,
        lpt_df.decision_npv,
        lpt_df.first_loan_npv,
        lpt_df.lifetime_npv,
        lpt_df.discount_rate,
        lpt_df.first_loan_roe,
        lpt_df.first_loan_cash,
        lpt_df.lifetime_roe,
        lpt_df.lifetime_cash,
        lpt_df.contribution_margin,
        lpt_df.total_fga_rate,
        lpt_df.fga_comission_rate,
        lpt_df.fga_client_rate,
        lpt_df.stage_loan_proposals_t
    FROM ls_df
    LEFT JOIN lpt_df ON ls_df.loan_id = lpt_df.loan_id

),
loan_status_trusted_df_2 AS (

    SELECT
        *,
        CASE
            WHEN first_payment_date = payday THEN 0
            WHEN first_payment_date != payday THEN
                CASE
                WHEN dayofmonth(calculation_date) > dayofmonth(first_payment_date)
                    THEN (year(calculation_date)*12 + month(calculation_date)) - (year(first_payment_date)*12 + month(first_payment_date)) + 1
                WHEN dayofmonth(calculation_date) <= dayofmonth(first_payment_date)
                    THEN (year(calculation_date)*12 + month(calculation_date)) - (year(first_payment_date)*12 + month(first_payment_date))
                END
        END AS mob,
        ((year(origination_date) * 100) + month(origination_date)) AS orig_yrmo,
        unpaid_principal + total_principal_paid AS total_principal,
        CASE
            WHEN (first_payment_date = payday) THEN origination_date
            ELSE (payday - INTERVAL '1' MONTH)
        END AS prev_payday
    FROM loan_status_trusted_df_1

),
apps_client_df AS (

    SELECT
        originations.client_id,
        client_loans.loan_id,
        applications.ally_slug AS store_name,
        applications.requested_amount,
        originations.approved_amount
    FROM originations
    LEFT JOIN applications
        ON originations.application_id = applications.application_id
    LEFT JOIN client_loans
        ON originations.application_id = client_loans.application_id

),
loan_status_trusted_df_3 AS (

    SELECT
        ls.*,
        apps.store_name,
        apps.requested_amount,
        apps.approved_amount
    FROM loan_status_trusted_df_2 ls
    LEFT JOIN apps_client_df apps ON ls.client_id = apps.client_id AND ls.loan_id = apps.loan_id

),
allies_df AS (

    SELECT DISTINCT
        ally_slug,
        ally_vertical:['name']:['value'] AS vertical_name,
        ally_vertical:['slug']:['value'] AS vertical_slug,
        ally_vertical:['isNew']:['value'] AS vertical_is_new
    FROM allies

),
loan_status_trusted_df AS (

    SELECT
        *,
        CAST(DAY(calculation_date) AS string) AS _day,
        CAST(YEAR(calculation_date) AS string) AS _year,
        CAST(MONTH(calculation_date) AS string) AS _month,
        DATE_FORMAT(calculation_date, 'yyyy-MM-dd') AS _dt
    FROM loan_status_trusted_df_3 ls
    LEFT JOIN allies_df al ON ls.ally_name = al.ally_slug

)

SELECT
    calculation_date,
    _day,
    _year,
    _month,
    _dt,
    loan_id,
    client_id,
    accrued_interest_this_month,
    applicable_rate,
    condoned_interest_this_month,
    current_installment_amount,
    current_interest_covered_this_month,
    current_interest_covered_this_period,
    current_interest_due_this_period,
    current_principal_covered_this_month,
    current_principal_due_this_period,
    days_past_due,
    full_payment,
    initial_installment_amount,
    interest_condoned_in_first_period,
    interest_on_overdue_principal,
    interest_on_overdue_principal_covered_this_month,
    interest_on_overdue_principal_covered_this_period,
    interest_overdue,
    interest_overdue_covered_this_month,
    interest_overdue_covered_this_period,
    ipmt,
    is_fully_paid,
    min_payment,
    months_on_books,
    paid_installments,
    payday,
    period_prepayment_benefit,
    ppmt,
    principal_overdue,
    principal_overdue_covered_this_month,
    principal_overdue_covered_this_period,
    total_current_interest_condoned,
    total_current_principal_condoned,
    total_interest_overdue_condoned,
    total_interest_paid,
    total_moratory_interest_condoned,
    total_payment_applied,
    total_prepayment_benefit,
    total_principal_overdue_condoned,
    total_principal_paid,
    total_unpaid_principal_condoned,
    unpaid_interest,
    unpaid_principal,
    guarantee_overdue,
    current_guarantee_due_this_period,
    unpaid_guarantee,
    total_guarantee_paid,
    guarantee_covered_this_period,
    guarantee_covered_this_month,
    total_guarantee_condoned,
    vintage,
    channel_str_agg,
    payments_ownership_agg,
    num_payments,
    total_pmt,
    principal_pmt,
    avg_pmt,
    stdev_pmt,
    first_payment_date,
    origination_date,
    cancellation_reason,
    refinancing_process_start_date,
    refinancing_process_end_date,
    refinancing_process_type,
    refinancing_process_annulled,
    refinancing_process_annullment_reason,
    refinancing_order,
    state,
    loan_proposal_id,
    ally_mdf,
    ally_name,
    interest_rate,
    lbl,
    learning_population,
    decision_npv,
    first_loan_npv,
    lifetime_npv,
    discount_rate,
    first_loan_roe,
    first_loan_cash,
    lifetime_roe,
    lifetime_cash,
    contribution_margin,
    total_fga_rate,
    fga_comission_rate,
    fga_client_rate,
    mob,
    orig_yrmo,
    total_principal,
    prev_payday,
    store_name,
    vertical_name AS vertical,
    requested_amount,
    approved_amount,
    stage_loan_proposals_t
FROM loan_status_trusted_df;