
--- QUERY CREATED BY: JOINT EFFORT - Martin Alalu (Data & Product) + Carlos D.A. Puerto N. + Alexis de la Fuente
----                                 (AE - Data team)
--- Description: Aggregated datamart with a calendar-relative logic which provides all necessary inputs for building
---              C-level / strategic dashboards. Uses two macros created for this specific purpose
--- LAST UPDATE: @2023-02-24

WITH br_bad_book AS (
    SELECT
        client_id,
        'seg1' AS segment
    FROM risk.seg1_cut_br_v2
    UNION ALL
    SELECT
        client_id,
        'seg2' AS segment
    FROM risk.seg2_cut_br_v2
)
, pago_fpd_br AS (
    SELECT
        date(date_trunc('DAY',r.fp_date_plus_1_month)) as dpd31_date,
        r.loan_id,
        s.segment,
        r.term,
        r.d_vintage,
        r.addi_pd,
        r.client_type,
        r.DPD_plus_1_month,
        r.UPB_plus_1_month,
        CAST(lp.total_interest AS NUMERIC) AS total_interest,
        case when r.dpd_plus_1_month>1 then r.UPB_plus_1_month when r.dpd_plus_1_month<=1 then 0 else null end dq31_at_31_upb_total,
        case when r.dpd_plus_1_month is not null then r.general_amount end dq31_at_31_opb_total
    FROM gold.risk_master_table_br AS r
    LEFT JOIN                br_bad_book AS s   ON r.prospect_id = s.client_id
    LEFT JOIN silver.f_loan_proposals_br AS lp  ON r.loan_id = lp.loan_proposal_id
    WHERE 1=1
        AND r.product like '%PAGO%'
        AND r.fp_date_plus_1_month >= date_trunc('month',current_date()) - interval '13 month'
        AND r.dpd_plus_1_month is not null
        AND r.loan_id is not null
 )
 , daily_fpd_31_at_31_pago_perc_br AS (
    SELECT
        'BR' AS country_code,
        date_trunc('day',dpd31_date) as period,
        SUM(dq31_at_31_upb_total) FILTER (WHERE segment IS NULL AND client_type = 'CLIENT') AS dq31_at_31_rc_good_num,
        SUM(dq31_at_31_opb_total) FILTER (WHERE segment IS NULL AND client_type = 'CLIENT') AS dq31_at_31_rc_good_den,

        SUM(dq31_at_31_upb_total) FILTER (WHERE segment IS NOT NULL AND client_type = 'CLIENT') AS  dq31_at_31_rc_bad_num,
        SUM(dq31_at_31_opb_total) FILTER (WHERE segment IS NOT NULL AND client_type = 'CLIENT') AS dq31_at_31_rc_bad_den,

        SUM(dq31_at_31_upb_total) FILTER (WHERE client_type = 'PROSPECT') AS dq31_at_31_prospect_num,
        SUM(dq31_at_31_opb_total) FILTER (WHERE client_type = 'PROSPECT') AS dq31_at_31_prospect_den,

        SUM(dq31_at_31_upb_total) FILTER (WHERE (segment IS NULL AND client_type = 'CLIENT') OR (client_type = 'PROSPECT')) AS dq31_at_31_rc_good_prospect_num,
        SUM(dq31_at_31_opb_total) FILTER (WHERE (segment IS NULL AND client_type = 'CLIENT') OR (client_type = 'PROSPECT')) AS dq31_at_31_rc_good_prospect_den,

        SUM(dq31_at_31_upb_total) FILTER (WHERE (segment IS NULL AND client_type = 'CLIENT' AND total_interest > 0) OR (client_type = 'PROSPECT' AND total_interest > 0)) AS dq31_at_31_rc_good_prospect_flex_num,
        SUM(dq31_at_31_opb_total) FILTER (WHERE (segment IS NULL AND client_type = 'CLIENT' AND total_interest > 0) OR (client_type = 'PROSPECT' AND total_interest > 0)) AS dq31_at_31_rc_good_prospect_flex_den
    FROM pago_fpd_br
    WHERE 1=1
        AND dpd31_date < (SELECT today FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO')))
    GROUP BY 1,2
)
, daily_fpd_31_at_31_pago_perc_co AS (
    SELECT
        period,
        country_code,
        dq31_at_31_prospect_num + dq31_at_31_prospect_flex_num AS dq31_at_31_prospect_num,
        dq31_at_31_prospect_den + dq31_at_31_prospect_flex_den AS dq31_at_31_prospect_den,
        dq31_at_31_rc_num + dq31_at_31_rc_flex_num AS dq31_at_31_rc_num,
        dq31_at_31_rc_den + dq31_at_31_rc_flex_den AS dq31_at_31_rc_den,
        dq31_at_31_flex_num,
        dq31_at_31_flex_den,
        (dq31_at_31_prospect_num + dq31_at_31_rc_num + dq31_at_31_flex_num) AS dq31_at_31_rc_and_prospect_num,
        (dq31_at_31_prospect_den + dq31_at_31_rc_den + dq31_at_31_flex_den) AS dq31_at_31_rc_and_prospect_den
    FROM(
        SELECT
            dpd31_date AS period,
            country_code,
            COALESCE(SUM(COALESCE(dq31_at_31_upb,0)) FILTER (WHERE client_type='PROSPECT'),0)      AS dq31_at_31_prospect_num,
            COALESCE(SUM(COALESCE(dq31_at_31_opb,0)) FILTER (WHERE client_type='PROSPECT'),0)      AS dq31_at_31_prospect_den,
            COALESCE(SUM(COALESCE(dq31_at_31_upb,0)) FILTER (WHERE client_type='CLIENT'),0)        AS dq31_at_31_rc_num,
            COALESCE(SUM(COALESCE(dq31_at_31_opb,0)) FILTER (WHERE client_type='CLIENT'),0)        AS dq31_at_31_rc_den,
            COALESCE(SUM(COALESCE(dq31_at_31_upb_flex,0)),0)                                       AS dq31_at_31_flex_num,
            COALESCE(SUM(COALESCE(dq31_at_31_opb_flex,0)),0)                                       AS dq31_at_31_flex_den,
            COALESCE(SUM(COALESCE(dq31_at_31_upb_flex,0)) FILTER (WHERE client_type='PROSPECT'),0) AS dq31_at_31_prospect_flex_num,
            COALESCE(SUM(COALESCE(dq31_at_31_opb_flex,0)) FILTER (WHERE client_type='PROSPECT'),0) AS dq31_at_31_prospect_flex_den,
            COALESCE(SUM(COALESCE(dq31_at_31_upb_flex,0)) FILTER (WHERE client_type='CLIENT'),0)   AS dq31_at_31_rc_flex_num,
            COALESCE(SUM(COALESCE(dq31_at_31_opb_flex,0)) FILTER (WHERE client_type='CLIENT'),0)   AS dq31_at_31_rc_flex_den
        FROM gold.agg_addi_okr_metrics_fpd_complete_pago
        WHERE 1 = 1
            AND country_code = 'CO'
            AND dpd31_date < (SELECT today FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO')))
        GROUP BY 1,2
    )
)
, total_losses AS (
    SELECT
        period,
        country_code,
        dq31_at_31_rc_and_prospect_num,
        dq31_at_31_rc_and_prospect_den
    FROM daily_fpd_31_at_31_pago_perc_co

    UNION ALL

    SELECT
        period,
        country_code,
        dq31_at_31_rc_good_prospect_num AS dq31_at_31_rc_and_prospect_num,
        dq31_at_31_rc_good_prospect_den AS dq31_at_31_rc_and_prospect_den
    FROM daily_fpd_31_at_31_pago_perc_br
)
,
addi_active_fx_rates (
    SELECT
        FIRST_VALUE(price) FILTER (WHERE country_code='CO') AS fx_rate_cop_usd,
        FIRST_VALUE(price) FILTER (WHERE country_code='BR') AS fx_rate_brl_usd
    FROM silver.d_fx_rate WHERE is_active
)
,
daily_mdf AS (
    SELECT
        period,
        country_code,
        segment,
        gmv_usd,
        --mdf_nominator,
        --mdf_denominator,
        mdf_fee_pago_co + lead_gen_fee_co AS mdf_lead_gen_fee_pago_co,
        gmv_pago_co,
        mdf_fee_pago_br + lead_gen_fee_br AS mdf_lead_gen_fee_pago_br,
        gmv_pago_br,
        (gmv_pago_br+gmv_pago_co) as gmv_pago,
        total_interest_pago_br + fga_pago_br AS interest_fga_pago_br,
        total_interest_pago_co + fga_pago_co AS interest_fga_pago_co,
        (mdf_fee_pago_br + total_interest_pago_br + lead_gen_fee_br + fga_pago_br) as total_take_rate_br,
        (mdf_fee_pago_co + total_interest_pago_co + lead_gen_fee_co + fga_pago_co) as total_take_rate_co,
        (mdf_fee_pago_co + total_interest_pago_co + mdf_fee_pago_br + total_interest_pago_br + lead_gen_fee_co + fga_pago_co + lead_gen_fee_br + fga_pago_br) as total_take_rate,
        gmv_pago_flex_br,
        gmv_pago_flex_co,
        addishop_paying_merchants_gmv_co,
        gmv_financia_co
    FROM(
        SELECT
            period,
            country_code,
            segment,
            ROUND(SUM(CASE WHEN country_code = 'BR' THEN COALESCE(gmv,0)/fx.fx_rate_brl_usd
                           WHEN country_code = 'CO' THEN COALESCE(gmv,0)/fx.fx_rate_cop_usd
                      END),2)                        AS gmv_usd,
            --SUM(COALESCE(dmf_nominator,0))         AS mdf_nominator,
            --SUM(COALESCE(dmf_denominator,0))       AS mdf_denominator,
            SUM(COALESCE(mdf_nominator_pago_co,0))   AS mdf_fee_pago_co,
            SUM(COALESCE(gmv_pago_co,0))             AS gmv_pago_co,
            SUM(COALESCE(mdf_nominator_pago_br,0))   AS mdf_fee_pago_br,
            SUM(COALESCE(gmv_pago_br,0))             AS gmv_pago_br,
            SUM(COALESCE(total_interest_pago_br,0))  AS total_interest_pago_br,
            SUM(COALESCE(total_interest_pago_co,0))  AS total_interest_pago_co,
            SUM(COALESCE(gmv_pago_flex_br,0))        AS gmv_pago_flex_br,
            SUM(COALESCE(gmv_pago_flex_co,0))        AS gmv_pago_flex_co,
            --SUM(COALESCE(marketplace_gmv_co,0))    AS marketplace_gmv_co,
            SUM(COALESCE(fga_pago_co,0))             AS fga_pago_co,
            SUM(COALESCE(fga_pago_br,0))             AS fga_pago_br,
            SUM(COALESCE(paying_merchants_fee_co,0)) AS lead_gen_fee_co,
            SUM(COALESCE(paying_merchants_fee_br,0)) AS lead_gen_fee_br,
            SUM(COALESCE(paying_merchants_gmv_co,0)) AS addishop_paying_merchants_gmv_co,
            SUM(COALESCE(gmv_financia_co,0))         AS gmv_financia_co
        FROM gold.dm_addi_okr_metrics_imply as i
        LEFT JOIN br_bad_book               AS s  ON i.client_id = s.client_id
        LEFT JOIN addi_active_fx_rates      AS fx ON TRUE
        WHERE 1 = 1
            AND period <  (SELECT today FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO')))
            AND segment IS NULL --Only Good Book
        GROUP BY 1,2,3
    ) AS r0
)
, daily_active_customers AS (
    SELECT
        period,
        'RC' as type,
        country_code,
        o.client_id,
        segment
    FROM gold.dm_addi_okr_metrics AS o
    LEFT JOIN br_bad_book s
            ON s.client_id = o.client_id
    WHERE 1 = 1
        AND period < (SELECT today FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO')))
         AND rc_transacting = 1
         AND product in ('PAGO_CO','PAGO_BR')
    UNION ALL
    SELECT
        period,
        'Prospects' as type,
        country_code,
        o.client_id,
        segment
    FROM gold.dm_addi_okr_metrics AS o
    LEFT JOIN br_bad_book s
            ON s.client_id = o.client_id
    WHERE 1 = 1
        AND period < (SELECT today FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO')))
        AND new_customers_transacting = 1
        AND product in ('PAGO_CO','PAGO_BR')
)

, categories_results AS (
--LOSSES
    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil Prospect Losses (31 @ 31) (+Flex)' AS metric,
    'percentage' AS metric_type,
    'BR Prospect Losses 31@31' AS numerator_name,
    'BR Prospect loans general amount' AS denominator_name,
    False AS positive_change_is_good,
    20 AS ordered,
    6 AS Mar_23_target,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil Prospect Losses (31 @ 31) (+Flex), metric_type = percentage, num_name = BR Prospect Losses 31@31, den_name = BR Prospect loans general amount, num_column='numerator_exemplar'dq31_at_31_prospect_num, den_column = dq31_at_31_prospect_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 20, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 6}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8
    FROM      daily_fpd_31_at_31_pago_perc_br  AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1=1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil RC Losses (31 @ 31) - Good Book (+Flex)' AS metric,
    'percentage' AS metric_type,
    'BR RC Good Book Losses 31@31' AS numerator_name,
    'BR RC Good Book loans general amount' AS denominator_name,
    False AS positive_change_is_good,
    21 AS ordered,
    4 AS Mar_23_target,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_good_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_good_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil RC Losses (31 @ 31) - Good Book (+Flex), metric_type = percentage, num_name = BR RC Good Book Losses 31@31, den_name = BR RC Good Book loans general amount, num_column='numerator_exemplar'dq31_at_31_rc_good_num, den_column = dq31_at_31_rc_good_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 21, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 4}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8
    FROM      daily_fpd_31_at_31_pago_perc_br AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1=1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Memo: Brazil RC Losses (31 @ 31) - Bad Book (+Flex)' AS metric,
    'percentage' AS metric_type,
    'BR RC Bad Book Losses 31@31' AS numerator_name,
    'BR RC Bad Book loans general amount' AS denominator_name,
    False AS positive_change_is_good,
    24 AS ordered,
    NULL AS Mar_23_target,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_bad_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_bad_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Memo: Brazil RC Losses (31 @ 31) - Bad Book (+Flex), metric_type = percentage, num_name = BR RC Bad Book Losses 31@31, den_name = BR RC Bad Book loans general amount, num_column='numerator_exemplar'dq31_at_31_rc_bad_num, den_column = dq31_at_31_rc_bad_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 24, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 'NULL'}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_br   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Memo: Brazil Flex Losses (31 @ 31) - Good Book' AS metric,
    'percentage' AS metric_type,
    'BR Flex Good Book Losses 31@31' AS numerator_name,
    'BR FLex Good Book loans general amount' AS denominator_name,
    False AS positive_change_is_good,
    23 AS ordered,
    6.8 AS Mar_23_target,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_good_prospect_flex_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Memo: Brazil Flex Losses (31 @ 31) - Good Book, metric_type = percentage, num_name = BR Flex Good Book Losses 31@31, den_name = BR FLex Good Book loans general amount, num_column='numerator_exemplar'dq31_at_31_rc_good_prospect_flex_num, den_column = dq31_at_31_rc_good_prospect_flex_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 23, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 6.8}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_br   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil Total Losses (31 @ 31)' AS metric,
    'percentage' AS metric_type,
    'BR Total Good Book Losses 31@31' AS numerator_name,
    'BR Total Good Book loans general amount' AS denominator_name,
    False AS positive_change_is_good,
    22 AS ordered,
    5.6 AS Mar_23_target,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_good_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_good_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil Total Losses (31 @ 31), metric_type = percentage, num_name = BR Total Good Book Losses 31@31, den_name = BR Total Good Book loans general amount, num_column='numerator_exemplar'dq31_at_31_rc_good_prospect_num, den_column = dq31_at_31_rc_good_prospect_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 22, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 5.6}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_br   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia Prospect Losses (31 @ 31) (+Flex)' AS metric,
    'percentage' AS metric_type,
    'CO Prospect Losses 31@31' AS numerator_name,
    'CO Prospect loans requested amount' AS denominator_name,
    False AS positive_change_is_good,
    25 AS ordered,
    7 AS Mar_23_target,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia Prospect Losses (31 @ 31) (+Flex), metric_type = percentage, num_name = CO Prospect Losses 31@31, den_name = CO Prospect loans requested amount, num_column='numerator_exemplar'dq31_at_31_prospect_num, den_column = dq31_at_31_prospect_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 25, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 7}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_co   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia RC Losses (31 @ 31) (+Flex)' AS metric,
    'percentage' AS metric_type,
    'CO RC Losses 31@31' AS numerator_name,
    'CO RC loans requested amount' AS denominator_name,
    False AS positive_change_is_good,
    26 AS ordered,
    3 AS Mar_23_target,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia RC Losses (31 @ 31) (+Flex), metric_type = percentage, num_name = CO RC Losses 31@31, den_name = CO RC loans requested amount, num_column='numerator_exemplar'dq31_at_31_rc_num, den_column = dq31_at_31_rc_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 26, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 3}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_co   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Memo: Colombia Flex Losses (31 @ 31) (+Flex)' AS metric,
    'percentage' AS metric_type,
    'CO Flex Losses 31@31' AS numerator_name,
    'CO Flex loans requested amount' AS denominator_name,
    False AS positive_change_is_good,
    28 AS ordered,
    5.5 AS Mar_23_target,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_flex_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_flex_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Memo: Colombia Flex Losses (31 @ 31) (+Flex), metric_type = percentage, num_name = CO Flex Losses 31@31, den_name = CO Flex loans requested amount, num_column='numerator_exemplar'dq31_at_31_flex_num, den_column = dq31_at_31_flex_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 28, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 5.5}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_co   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia Total Losses (31 @ 31)' AS metric,
    'percentage' AS metric_type,
    'CO Total Losses 31@31' AS numerator_name,
    'CO Total loans requested amount' AS denominator_name,
    False AS positive_change_is_good,
    27 AS ordered,
    4.5 AS Mar_23_target,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia Total Losses (31 @ 31), metric_type = percentage, num_name = CO Total Losses 31@31, den_name = CO Total loans requested amount, num_column='numerator_exemplar'dq31_at_31_rc_and_prospect_num, den_column = dq31_at_31_rc_and_prospect_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 27, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 4.5}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_fpd_31_at_31_pago_perc_co   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'ADDI Total Losses (31 @ 31)' AS metric,
    'percentage' AS metric_type,
    'Addi Total Losses 31@31' AS numerator_name,
    'Addi Total loans requested amount' AS denominator_name,
    False AS positive_change_is_good,
    3 AS ordered,
    4.5 AS Mar_23_target,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.dq31_at_31_rc_and_prospect_num) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.dq31_at_31_rc_and_prospect_den) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = ADDI Total Losses (31 @ 31), metric_type = percentage, num_name = Addi Total Losses 31@31, den_name = Addi Total loans requested amount, num_column='numerator_exemplar'dq31_at_31_rc_and_prospect_num, den_column = dq31_at_31_rc_and_prospect_den, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 3, positive_change_is_good = False, extra_column_and_values = {'Mar_23_target': 4.5}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      total_losses   AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    GROUP BY 1,2,3,4,5,6,7

--TAKE RATE

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil Pago Merchant Take Rate' AS metric,
    'percentage' AS metric_type,
    'BR MDF + LGF Revenue' AS numerator_name,
    'BR Pago Good Book GMV' AS denominator_name,
    True AS positive_change_is_good,
    9 AS ordered,
    5 AS Mar_23_target,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.mdf_lead_gen_fee_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil Pago Merchant Take Rate, metric_type = percentage, num_name = BR MDF + LGF Revenue, den_name = BR Pago Good Book GMV, num_column='numerator_exemplar'mdf_lead_gen_fee_pago_br, den_column = gmv_pago_br, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 9, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 5}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil Pago Consumer Take Rate' AS metric,
    'percentage' AS metric_type,
    'BR Interest + FGA Revenue' AS numerator_name,
    'BR Pago Good Book GMV' AS denominator_name,
    True AS positive_change_is_good,
    10 AS ordered,
    4 AS Mar_23_target,
    SUM(m.interest_fga_pago_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.interest_fga_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil Pago Consumer Take Rate, metric_type = percentage, num_name = BR Interest + FGA Revenue, den_name = BR Pago Good Book GMV, num_column='numerator_exemplar'interest_fga_pago_br, den_column = gmv_pago_br, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 10, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 4}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Total Brazil Take Rate' AS metric,
    'percentage' AS metric_type,
    'BR MDF + LGF + Total Interest' AS numerator_name,
    'BR Pago Good Book GMV' AS denominator_name,
    True AS positive_change_is_good,
    11 AS ordered,
    9 AS Mar_23_target,
    SUM(m.total_take_rate_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.total_take_rate_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.total_take_rate_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.total_take_rate_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.total_take_rate_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.total_take_rate_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.total_take_rate_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.total_take_rate_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.total_take_rate_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.total_take_rate_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.total_take_rate_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Total Brazil Take Rate, metric_type = percentage, num_name = BR MDF + LGF + Total Interest, den_name = BR Pago Good Book GMV, num_column='numerator_exemplar'total_take_rate_br, den_column = gmv_pago_br, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 11, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 9}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia Pago Merchant Take Rate' AS metric,
    'percentage' AS metric_type,
    'CO MDF + LGF Revenue' AS numerator_name,
    'CO Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    12 AS ordered,
    3.5 AS Mar_23_target,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.mdf_lead_gen_fee_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia Pago Merchant Take Rate, metric_type = percentage, num_name = CO MDF + LGF Revenue, den_name = CO Pago GMV, num_column='numerator_exemplar'mdf_lead_gen_fee_pago_co, den_column = gmv_pago_co, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 12, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 3.5}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia Pago Consumer Take Rate' AS metric,
    'percentage' AS metric_type,
    'CO Total Interest + FGA revenue' AS numerator_name,
    'CO Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    13 AS ordered,
    4 AS Mar_23_target,
    SUM(m.interest_fga_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.interest_fga_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia Pago Consumer Take Rate, metric_type = percentage, num_name = CO Total Interest + FGA revenue, den_name = CO Pago GMV, num_column='numerator_exemplar'interest_fga_pago_co, den_column = gmv_pago_co, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 13, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 4}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Total Colombia Take Rate' AS metric,
    'percentage' AS metric_type,
    'CO MDF + LGF + Total Interest' AS numerator_name,
    'CO Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    14 AS ordered,
    7 AS Mar_23_target,
    SUM(m.total_take_rate_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.total_take_rate_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.total_take_rate_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.total_take_rate_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.total_take_rate_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.total_take_rate_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.total_take_rate_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.total_take_rate_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.total_take_rate_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.total_take_rate_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.total_take_rate_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Total Colombia Take Rate, metric_type = percentage, num_name = CO MDF + LGF + Total Interest, den_name = CO Pago GMV, num_column='numerator_exemplar'total_take_rate_co, den_column = gmv_pago_co, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 14, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 7}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Total ADDI Take Rate' AS metric,
    'percentage' AS metric_type,
    'Addi MDF + LGF + Total Interest' AS numerator_name,
    'Addi Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    15 AS ordered,
    7 AS Mar_23_target,
    SUM(m.total_take_rate) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.total_take_rate) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.total_take_rate) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.total_take_rate) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.total_take_rate) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.total_take_rate) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.total_take_rate) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.total_take_rate) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.total_take_rate) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.total_take_rate) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.total_take_rate) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Total ADDI Take Rate, metric_type = percentage, num_name = Addi MDF + LGF + Total Interest, den_name = Addi Pago GMV, num_column='numerator_exemplar'total_take_rate, den_column = gmv_pago, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 15, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 7}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    GROUP BY 1,2,3,4,5,6,7

UNION ALL

--GMV

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil GMV - (USD) (Good Book)' AS metric,
    'money' AS metric_type,
    'BR Good Book GMV (usd)' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    4 AS ordered,
    5000000 AS Mar_23_target,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num, 
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil GMV - (USD) (Good Book), metric_type = money, num_name = BR Good Book GMV (usd), den_name = NA, num_column='numerator_exemplar'gmv_usd, den_column = None, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 4, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 5000000}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_mdf          AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
        AND segment IS NULL --Keeping only Good Clients
    GROUP BY 1,2,3,4,5

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    '% GMV on Flex Brazil' AS metric,
    'percentage' AS metric_type,
    'BR Flex GMV' AS numerator_name,
    'BR Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    16 AS ordered,
    25 AS Mar_23_target,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.gmv_pago_flex_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_br) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = % GMV on Flex Brazil, metric_type = percentage, num_name = BR Flex GMV, den_name = BR Pago GMV, num_column='numerator_exemplar'gmv_pago_flex_br, den_column = gmv_pago_br, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 16, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 25}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia GMV (USD)' AS metric,
    'money' AS metric_type,
    'CO GMV (usd)' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    5 AS ordered,
    20000000 AS Mar_23_target,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num, 
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia GMV (USD), metric_type = money, num_name = CO GMV (usd), den_name = NA, num_column='numerator_exemplar'gmv_usd, den_column = None, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 5, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 20000000}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_mdf          AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    '% GMV on Flex Colombia' AS metric,
    'percentage' AS metric_type,
    'CO Flex GMV' AS numerator_name,
    'CO Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    17 AS ordered,
    25 AS Mar_23_target,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.gmv_pago_flex_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = % GMV on Flex Colombia, metric_type = percentage, num_name = CO Flex GMV, den_name = CO Pago GMV, num_column='numerator_exemplar'gmv_pago_flex_co, den_column = gmv_pago_co, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 17, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 25}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    '% GMV on AddiShop Colombia' AS metric,
    'percentage' AS metric_type,
    'CO AddiShop Paying Merchants GMV' AS numerator_name,
    'CO Pago GMV' AS denominator_name,
    True AS positive_change_is_good,
    18 AS ordered,
    NULL AS Mar_23_target,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.addishop_paying_merchants_gmv_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_den,
    SUM(m.gmv_pago_co) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_den
/* DEBUGGING
 metric_name = % GMV on AddiShop Colombia, metric_type = percentage, num_name = CO AddiShop Paying Merchants GMV, den_name = CO Pago GMV, num_column='numerator_exemplar'addishop_paying_merchants_gmv_co, den_column = gmv_pago_co, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 18, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 'NULL'}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_mdf AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Total GMV (USD) (Good Book)' AS metric,
    'money' AS metric_type,
    'Addi gmv (usd)' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    6 AS ordered,
    25000000 AS Mar_23_target,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    SUM(m.gmv_usd) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num, 
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Total GMV (USD) (Good Book), metric_type = money, num_name = Addi gmv (usd), den_name = NA, num_column='numerator_exemplar'gmv_usd, den_column = None, date_column = period, num_function = SUM, den_function = SUM, num_function_add_distinct = False, den_function_add_distinct = False, order_num = 6, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 25000000}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM      daily_mdf          AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1=1
        AND segment IS NULL --Keeping only Good Clients
    GROUP BY 1,2,3,4,5

--RC TRANSACTING

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Brazil Pago RC Transacting (Good Book)' AS metric,
    'number' AS metric_type,
    'BR Pago (BNPL) RC Good Book Transacting' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    6 AS ordered,
    20000 AS Mar_23_target,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num, 
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Brazil Pago RC Transacting (Good Book), metric_type = number, num_name = BR Pago (BNPL) RC Good Book Transacting, den_name = NA, num_column='numerator_exemplar'client_id, den_column = None, date_column = period, num_function = COUNT, den_function = SUM, num_function_add_distinct = True, den_function_add_distinct = False, order_num = 6, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 20000}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_active_customers AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'BR'
        AND type = 'RC'
        AND segment IS NULL --Only Good book
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Colombia Pago RC Transacting' AS metric,
    'number' AS metric_type,
    'CO Pago RC Transacting' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    7 AS ordered,
    230000 AS Mar_23_target,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num, 
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Colombia Pago RC Transacting, metric_type = number, num_name = CO Pago RC Transacting, den_name = NA, num_column='numerator_exemplar'client_id, den_column = None, date_column = period, num_function = COUNT, den_function = SUM, num_function_add_distinct = True, den_function_add_distinct = False, order_num = 7, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 230000}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_active_customers AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1 = 1
        AND country_code = 'CO'
        AND type = 'RC'
    GROUP BY 1,2,3,4,5,6,7

UNION ALL

    SELECT
-- AUTOGENERATED CODE START
    d.today,
    'Total Pago RC Transacting (Good Book)' AS metric,
    'number' AS metric_type,
    'Total Pago RC Transacting' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    8 AS ordered,
    250000 AS Mar_23_target,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period = d.yesterday)                                                              AS yesterday_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.current_week_start AND d.yesterday END)                       AS wtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.current_month_start AND d.yesterday END)                      AS mtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.period BETWEEN d.current_quarter_start AND d.yesterday END)                    AS qtd_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_week_start AND d.last_week_end)                          AS last_week_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_last_week_start AND d.last_last_week_end)                AS last_last_week_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.3_weeks_ago_start AND d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE                                            m.period BETWEEN d.last_month_start AND d.last_month_end)                        AS last_month_complete_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_week_start AND d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.period BETWEEN d.last_last_week_start AND d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    COUNT(DISTINCT m.client_id) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.period BETWEEN d.last_month_start AND d.last_month_end_equivalent END)         AS last_month_equivalent_num, 
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
 metric_name = Total Pago RC Transacting (Good Book), metric_type = number, num_name = Total Pago RC Transacting, den_name = NA, num_column='numerator_exemplar'client_id, den_column = None, date_column = period, num_function = COUNT, den_function = SUM, num_function_add_distinct = True, den_function_add_distinct = False, order_num = 8, positive_change_is_good = True, extra_column_and_values = {'Mar_23_target': 250000}, m = m, d = d
 */
 -- AUTOGENERATED CODE END; USE -> GROUP BY 1,2,3,4,5,6,7,8

    FROM daily_active_customers AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    WHERE 1=1
        AND type = 'RC'
        AND segment IS NULL --Keep only good book
    GROUP BY 1,2,3,4,5,6,7

--ADDISHOP PAYING MERCHANTS
UNION ALL

    SELECT
-- AUTOGENERATED CODE (MANUALLY MODIFIED) START
    d.today,
    '# of Paying Merchants' AS metric,
    'number' AS metric_type,
    '# of AddiShop Paying Merchants' AS numerator_name,
    'NA' AS denominator_name,
    True AS positive_change_is_good,
    19 AS ordered,
    100 AS Mar_23_target,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE                                        m.start_date <= d.yesterday AND m.end_date > d.yesterday)                                  AS yesterday_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.start_date <= d.yesterday AND m.end_date > d.yesterday END)                              AS wtd_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.start_date <= d.yesterday AND m.end_date > d.yesterday END)                              AS mtd_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE CASE WHEN d.doq     = 1 THEN NULL ELSE m.start_date <= d.yesterday AND m.end_date > d.yesterday END)                              AS qtd_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE                                        m.start_date <= d.last_week_end AND m.end_date > d.last_week_end)                          AS last_week_complete_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE                                        m.start_date <= d.last_last_week_end AND m.end_date > d.last_last_week_end)                AS last_last_week_complete_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE                                        m.start_date <= d.3_weeks_ago_end AND m.end_date > d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE                                        m.start_date <= d.last_month_end AND m.end_date > d.last_month_end)                        AS last_month_complete_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.start_date <= d.last_week_end AND m.end_date > d.last_week_end_equivalent END)           AS last_week_equivalent_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE CASE WHEN d.dow_iso = 1 THEN NULL ELSE m.start_date <= d.last_last_week_end AND m.end_date > d.last_last_week_end_equivalent END) AS last_last_week_equivalent_num,
    COUNT(DISTINCT m.ally_slug) FILTER (WHERE CASE WHEN d.dom     = 1 THEN NULL ELSE m.start_date <= d.last_month_end AND m.end_date > d.last_month_end_equivalent END)         AS last_month_equivalent_num,
    FIRST_VALUE(1) AS yesterday_den,
    FIRST_VALUE(1) AS wtd_den,
    FIRST_VALUE(1) AS mtd_den,
    FIRST_VALUE(1) AS qtd_den,
    FIRST_VALUE(1) AS last_week_complete_den,
    FIRST_VALUE(1) AS last_last_week_complete_den,
    FIRST_VALUE(1) AS 3_weeks_ago_complete_den,
    FIRST_VALUE(1) AS last_month_complete_den,
    FIRST_VALUE(1) AS last_week_equivalent_den,
    FIRST_VALUE(1) AS last_last_week_equivalent_den,
    FIRST_VALUE(1) AS last_month_equivalent_den
/* DEBUGGING
  metric_name = # of Paying Merchants ;metric_type = number ;num_name = # of AddiShop Paying Merchants ;den_name = NA ;order_num = 19 ;extra_column_and_values = {'Mar_23_target': 100} ;num_column='nally_name ;den_column = None ;date_column = start_date ;num_function = COUNT ;den_function = SUM ;num_function_add_distinct = True ;den_function_add_distinct = False ;m = m ;d = d ;
 */
 -- AUTOGENERATED CODE (MANUALLY MODIFIED) END; USE -> GROUP BY 1,2,3,4,5,6,7

    FROM gold.dm_addishop_paying_allies_co AS m
    LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE

    GROUP BY 1,2,3,4,5,6,7
)

, ram_category_results AS (
    SELECT
        r.today,
        'Brazil RAM Leading Proxy (Good Book)' as metric,
        'percentage' AS metric_type,
        'BR RAM Leading Proxy (Good Book) (Losses - Revenue)' AS numerator_name,
        'NA' AS denominator_name,
        True AS positive_change_is_good,
        1 AS ordered,
        3 AS Mar_23_target,
        (r2.yesterday_num                 / NULLIF(r2.yesterday_den,0))                 - (r.yesterday_num                 / NULLIF(r.yesterday_den,0))                 AS yesterday_num,
        (r2.wtd_num                       / NULLIF(r2.wtd_den,0))                       - (r.wtd_num                       / NULLIF(r.wtd_den,0))                       AS wtd_num,
        (r2.mtd_num                       / NULLIF(r2.mtd_den,0))                       - (r.mtd_num                       / NULLIF(r.mtd_den,0))                       AS mtd_num,
        (r2.qtd_num                       / NULLIF(r2.qtd_den,0))                       - (r.qtd_num                       / NULLIF(r.qtd_den,0))                       AS qtd_num,
        (r2.last_week_complete_num        / NULLIF(r2.last_week_complete_den,0))        - (r.last_week_complete_num        / NULLIF(r.last_week_complete_den,0))        AS last_week_complete_num,
        (r2.last_last_week_complete_num   / NULLIF(r2.last_last_week_complete_den,0))   - (r.last_last_week_complete_num   / NULLIF(r.last_last_week_complete_den,0))   AS last_last_week_complete_num,
        (r2.3_weeks_ago_complete_num      / NULLIF(r2.3_weeks_ago_complete_den,0))      - (r.3_weeks_ago_complete_num      / NULLIF(r.3_weeks_ago_complete_den,0))      AS 3_weeks_ago_complete_num,
        (r2.last_month_complete_num       / NULLIF(r2.last_month_complete_den,0))       - (r.last_month_complete_num       / NULLIF(r.last_month_complete_den,0))       AS last_month_complete_num,
        (r2.last_week_equivalent_num      / NULLIF(r2.last_week_equivalent_den,0))      - (r.last_week_equivalent_num      / NULLIF(r.last_week_equivalent_den,0))      AS last_week_equivalent_num,
        (r2.last_last_week_equivalent_num / NULLIF(r2.last_last_week_equivalent_den,0)) - (r.last_last_week_equivalent_num / NULLIF(r.last_last_week_equivalent_den,0)) AS last_last_week_equivalent_num,
        (r2.last_month_equivalent_num     / NULLIF(r2.last_month_equivalent_den,0))     - (r.last_month_equivalent_num     / NULLIF(r.last_month_equivalent_den,0))     AS last_month_equivalent_num,
        1 AS yesterday_den,
        1 AS wtd_den,
        1 AS mtd_den,
        1 AS qtd_den,
        1 AS last_week_complete_den,
        1 AS last_last_week_complete_den,
        1 AS 3_weeks_ago_complete_den,
        1 AS last_month_complete_den,
        1 AS last_week_equivalent_den,
        1 AS last_last_week_equivalent_den,
        1 AS last_month_equivalent_den
    FROM      categories_results AS r
    LEFT JOIN categories_results AS r2
        ON r2.today = r.today
        AND r2.metric = 'Total Brazil Take Rate'
    --LEFT JOIN gold.dm_support_today_master_calendar AS d
    --    ON TRUE
    --    AND  ARRAYS_OVERLAP(countries,ARRAY('BR')) --
    WHERE 1=1
        AND r.metric = 'Brazil Total Losses (31 @ 31)'

    UNION ALL

    SELECT
        r.today,
        'Colombia RAM Leading Proxy' as metric,
        'percentage' AS metric_type,
        'CO RAM Leading Proxy (Losses - Revenue)' AS numerator_name,
        'NA' AS denominator_name,
        True AS positive_change_is_good,
        2 AS ordered,
        3 AS Mar_23_target,
        (r2.yesterday_num                 / NULLIF(r2.yesterday_den,0))                 - (r.yesterday_num                 / NULLIF(r.yesterday_den,0))                 AS yesterday_num,
        (r2.wtd_num                       / NULLIF(r2.wtd_den,0))                       - (r.wtd_num                       / NULLIF(r.wtd_den,0))                       AS wtd_num,
        (r2.mtd_num                       / NULLIF(r2.mtd_den,0))                       - (r.mtd_num                       / NULLIF(r.mtd_den,0))                       AS mtd_num,
        (r2.qtd_num                       / NULLIF(r2.qtd_den,0))                       - (r.qtd_num                       / NULLIF(r.qtd_den,0))                       AS qtd_num,
        (r2.last_week_complete_num        / NULLIF(r2.last_week_complete_den,0))        - (r.last_week_complete_num        / NULLIF(r.last_week_complete_den,0))        AS last_week_complete_num,
        (r2.last_last_week_complete_num   / NULLIF(r2.last_last_week_complete_den,0))   - (r.last_last_week_complete_num   / NULLIF(r.last_last_week_complete_den,0))   AS last_last_week_complete_num,
        (r2.3_weeks_ago_complete_num      / NULLIF(r2.3_weeks_ago_complete_den,0))      - (r.3_weeks_ago_complete_num      / NULLIF(r.3_weeks_ago_complete_den,0))      AS 3_weeks_ago_complete_num,
        (r2.last_month_complete_num       / NULLIF(r2.last_month_complete_den,0))       - (r.last_month_complete_num       / NULLIF(r.last_month_complete_den,0))       AS last_month_complete_num,
        (r2.last_week_equivalent_num      / NULLIF(r2.last_week_equivalent_den,0))      - (r.last_week_equivalent_num      / NULLIF(r.last_week_equivalent_den,0))      AS last_week_equivalent_num,
        (r2.last_last_week_equivalent_num / NULLIF(r2.last_last_week_equivalent_den,0)) - (r.last_last_week_equivalent_num / NULLIF(r.last_last_week_equivalent_den,0)) AS last_last_week_equivalent_num,
        (r2.last_month_equivalent_num     / NULLIF(r2.last_month_equivalent_den,0))     - (r.last_month_equivalent_num     / NULLIF(r.last_month_equivalent_den,0))     AS last_month_equivalent_num,
        1 AS yesterday_den,
        1 AS wtd_den,
        1 AS mtd_den,
        1 AS qtd_den,
        1 AS last_week_complete_den,
        1 AS last_last_week_complete_den,
        1 AS 3_weeks_ago_complete_den,
        1 AS last_month_complete_den,
        1 AS last_week_equivalent_den,
        1 AS last_last_week_equivalent_den,
        1 AS last_month_equivalent_den
    FROM      categories_results AS r
    LEFT JOIN categories_results AS r2
        ON r2.today = r.today
        AND r2.metric = 'Total Colombia Take Rate'
    --LEFT JOIN gold.dm_support_today_master_calendar AS d
    --    ON TRUE
    --    AND  ARRAYS_OVERLAP(countries,ARRAY('BR')) --
    WHERE 1=1
        AND r.metric = 'Colombia Total Losses (31 @ 31)'

    UNION ALL

    SELECT
        r.today,
        'ADDI RAM Leading Proxy' as metric,
        'percentage' AS metric_type,
        'ADDI RAM Leading Proxy (Losses - Revenue)' AS numerator_name,
        'NA' AS denominator_name,
        True AS positive_change_is_good,
        1 AS ordered,
        3 AS Mar_23_target,
        (r2.yesterday_num                 / NULLIF(r2.yesterday_den,0))                 - (r.yesterday_num                 / NULLIF(r.yesterday_den,0))                 AS yesterday_num,
        (r2.wtd_num                       / NULLIF(r2.wtd_den,0))                       - (r.wtd_num                       / NULLIF(r.wtd_den,0))                       AS wtd_num,
        (r2.mtd_num                       / NULLIF(r2.mtd_den,0))                       - (r.mtd_num                       / NULLIF(r.mtd_den,0))                       AS mtd_num,
        (r2.qtd_num                       / NULLIF(r2.qtd_den,0))                       - (r.qtd_num                       / NULLIF(r.qtd_den,0))                       AS qtd_num,
        (r2.last_week_complete_num        / NULLIF(r2.last_week_complete_den,0))        - (r.last_week_complete_num        / NULLIF(r.last_week_complete_den,0))        AS last_week_complete_num,
        (r2.last_last_week_complete_num   / NULLIF(r2.last_last_week_complete_den,0))   - (r.last_last_week_complete_num   / NULLIF(r.last_last_week_complete_den,0))   AS last_last_week_complete_num,
        (r2.3_weeks_ago_complete_num      / NULLIF(r2.3_weeks_ago_complete_den,0))      - (r.3_weeks_ago_complete_num      / NULLIF(r.3_weeks_ago_complete_den,0))      AS 3_weeks_ago_complete_num,
        (r2.last_month_complete_num       / NULLIF(r2.last_month_complete_den,0))       - (r.last_month_complete_num       / NULLIF(r.last_month_complete_den,0))       AS last_month_complete_num,
        (r2.last_week_equivalent_num      / NULLIF(r2.last_week_equivalent_den,0))      - (r.last_week_equivalent_num      / NULLIF(r.last_week_equivalent_den,0))      AS last_week_equivalent_num,
        (r2.last_last_week_equivalent_num / NULLIF(r2.last_last_week_equivalent_den,0)) - (r.last_last_week_equivalent_num / NULLIF(r.last_last_week_equivalent_den,0)) AS last_last_week_equivalent_num,
        (r2.last_month_equivalent_num     / NULLIF(r2.last_month_equivalent_den,0))     - (r.last_month_equivalent_num     / NULLIF(r.last_month_equivalent_den,0))     AS last_month_equivalent_num,
        1 AS yesterday_den,
        1 AS wtd_den,
        1 AS mtd_den,
        1 AS qtd_den,
        1 AS last_week_complete_den,
        1 AS last_last_week_complete_den,
        1 AS 3_weeks_ago_complete_den,
        1 AS last_month_complete_den,
        1 AS last_week_equivalent_den,
        1 AS last_last_week_equivalent_den,
        1 AS last_month_equivalent_den
    FROM      categories_results AS r
    LEFT JOIN categories_results AS r2
        ON r2.today = r.today
        AND r2.metric = 'Total ADDI Take Rate'
    --LEFT JOIN gold.dm_support_today_master_calendar AS d
    --    ON TRUE
    --    AND  ARRAYS_OVERLAP(countries,ARRAY('BR')) --
    WHERE 1=1
        AND r.metric = 'ADDI Total Losses (31 @ 31)'
)
, results AS (
    SELECT * FROM categories_results

UNION ALL

    SELECT * FROM ram_category_results

    ORDER BY ordered
)
SELECT
-- AUTOGENERATED CODE START
-- TODAY
d.today,
-- GENERAL METRIC REFERENCE
r.metric,
r.metric_type,
r.numerator_name,
r.denominator_name,
r.positive_change_is_good,
r.ordered,
-- ADDITIONAL COLUMNS
r.Mar_23_target,
-- METRIC CALCULATIONS
r.yesterday_num                 / NULLIF(r.yesterday_den,0)                 AS yesterday_complete,
r.wtd_num                       / NULLIF(r.wtd_den,0)                       AS wtd,
r.mtd_num                       / NULLIF(r.mtd_den,0)                       AS mtd,
r.qtd_num                       / NULLIF(r.qtd_den,0)                       AS qtd,
r.last_week_complete_num        / NULLIF(r.last_week_complete_den,0)        AS last_week_complete,
r.last_last_week_complete_num   / NULLIF(r.last_last_week_complete_den,0)   AS last_last_week_complete,
r.3_weeks_ago_complete_num      / NULLIF(r.3_weeks_ago_complete_den,0)      AS 3_weeks_ago_complete,
r.last_month_complete_num       / NULLIF(r.last_month_complete_den,0)       AS last_month_complete,
r.last_week_equivalent_num      / NULLIF(r.last_week_equivalent_den,0)      AS last_week_equivalent,
r.last_last_week_equivalent_num / NULLIF(r.last_last_week_equivalent_den,0) AS last_last_week_equivalent,
r.last_month_equivalent_num     / NULLIF(r.last_month_equivalent_den,0)     AS last_month_equivalent,
-- METRIC CALCULATIONS - DIFFERENCE OVER LAST PERIOD
CASE WHEN r.metric_type = 'percentage' THEN 'pp.' ELSE  '%' END AS period_over_period_unit,
CASE WHEN r.metric_type = 'percentage'
    THEN  (r.wtd_num                     / NULLIF(r.wtd_den,0))                     -       (r.last_week_equivalent_num    / NULLIF(r.last_week_equivalent_den,0))      -- percentage, pp
    ELSE ((r.wtd_num                     / NULLIF(r.wtd_den,0))                     / NULLIF(r.last_week_equivalent_num    / NULLIF(r.last_week_equivalent_den,0),0))-1 -- numeric,money; %
END AS wow_equivalent,
CASE WHEN r.metric_type = 'percentage'
    THEN  (r.last_week_complete_num      / NULLIF(r.last_week_complete_den,0))      -       (r.last_last_week_complete_num / NULLIF(r.last_last_week_complete_den,0))      -- percentage, pp
    ELSE ((r.last_week_complete_num      / NULLIF(r.last_week_complete_den,0))      / NULLIF(r.last_last_week_complete_num / NULLIF(r.last_last_week_complete_den,0),0))-1 -- numeric,money; %
END AS lw_o_llw_complete,
CASE WHEN r.metric_type = 'percentage'
    THEN  (r.last_last_week_complete_num / NULLIF(r.last_last_week_complete_den,0)) -       (r.3_weeks_ago_complete_num    / NULLIF(r.3_weeks_ago_complete_den,0))      -- percentage, pp
    ELSE ((r.last_last_week_complete_num / NULLIF(r.last_last_week_complete_den,0)) / NULLIF(r.3_weeks_ago_complete_num    / NULLIF(r.3_weeks_ago_complete_den,0),0))-1 -- numeric,money; %
END AS llw_o_3wa_complete,
CASE WHEN r.metric_type = 'percentage'
    THEN  (r.mtd_num                     / NULLIF(r.mtd_den,0))                     -       (r.last_month_equivalent_num   / NULLIF(r.last_month_equivalent_den,0))      -- percentage, pp
    ELSE ((r.mtd_num                     / NULLIF(r.mtd_den,0))                     / NULLIF(r.last_month_equivalent_num   / NULLIF(r.last_month_equivalent_den,0),0))-1 -- numeric,money; %
END AS mom_equivalent,
-- METRIC DETAILS - NUMERATORS AND DENOMINATORS
r.yesterday_num,
r.yesterday_den,
r.wtd_num,
r.wtd_den,
r.mtd_num,
r.mtd_den,
r.qtd_num,
r.qtd_den,
r.last_week_complete_num,
r.last_week_complete_den,
r.last_last_week_complete_num,
r.last_last_week_complete_den,
r.3_weeks_ago_complete_num,
r.3_weeks_ago_complete_den,
r.last_month_complete_num,
r.last_month_complete_den,
r.last_week_equivalent_num,
r.last_week_equivalent_den,
r.last_last_week_equivalent_num,
r.last_last_week_equivalent_den,
r.last_month_equivalent_num,
r.last_month_equivalent_den,
-- CALCULATION TIMEFRAMES AS STRING
CASE WHEN d.dow_iso = 1 THEN NULL ELSE CONCAT(d.current_week_start,'-',d.yesterday)                       END AS wtd_timeframe,
CASE WHEN d.dom = 1     THEN NULL ELSE CONCAT(d.current_month_start,'-',d.yesterday)                      END AS mtd_timeframe,
CASE WHEN d.doq = 1     THEN NULL ELSE CONCAT(d.current_quarter_start,'-',d.yesterday)                    END AS qtd_timeframe,
                                           CONCAT(d.last_week_start,'-',d.last_week_end)                          AS last_week_complete_timeframe,
                                           CONCAT(d.last_last_week_start,'-',d.last_last_week_end)                AS last_last_week_complete_timeframe,
                                           CONCAT(d.3_weeks_ago_start,'-',d.3_weeks_ago_end)                      AS 3_weeks_ago_complete_timeframe,
                                           CONCAT(d.last_month_start,'-',d.last_month_end)                        AS last_month_complete_timeframe,
CASE WHEN d.dow_iso = 1 THEN NULL ELSE CONCAT(d.last_week_start,'-',d.last_week_end_equivalent)           END AS last_week_equivalent_timeframe,
CASE WHEN d.dow_iso = 1 THEN NULL ELSE CONCAT(d.last_last_week_start,'-',d.last_last_week_end_equivalent) END AS last_last_week_equivalent_timeframe,
CASE WHEN d.dom = 1     THEN NULL ELSE CONCAT(d.last_month_start,'-',d.last_month_end_equivalent)         END AS last_month_equivalent_timeframe,
-- REFERENCES DATES TO TODAY
d.yesterday,
d.current_quarter_start,
d.current_month_start,
d.current_week_start,
d.`4_weeks_ago_start`,
d.`4_weeks_ago_end`,
d.`3_weeks_ago_start`,
d.`3_weeks_ago_end`,
d.last_last_week_start,
d.last_last_week_end,
d.last_week_start,
d.last_week_end,
d.last_week_end_equivalent,
d.last_last_week_end_equivalent,
d.last_month_start,
d.last_month_end,
d.last_month_end_equivalent,
d.dow_iso,
d.dom,
d.last_quarter_start,
d.last_quarter_end,
d.last_last_quarter_start,
d.last_last_quarter_end,
d.last_last_month_start,
d.last_last_month_end
/* DEBUGGING
 extra_column_and_values =  ; r = r ; d = d 
 */
-- AUTOGENERATED CODE END


    ,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at

FROM results as r
LEFT JOIN (SELECT * FROM gold.dm_support_today_master_calendar WHERE ARRAYS_OVERLAP(countries,ARRAY('CO'))) AS d ON TRUE