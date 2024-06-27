{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--        materialized='incremental',
--        unique_key='application_id',
--        incremental_strategy='merge',
-- AE - Alexis de la Fuente: Incremental approach as the one we use with the silver builder
WITH
{%- if is_incremental() %}
target_applications_co AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_co') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
target_applications_br AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_br') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_originations_bnpl_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpn_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpn_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpl_br AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_originations_bnpn_br AS (
    SELECT *
    FROM {{ ref('f_originations_bnpn_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_applications_co AS (
    SELECT *
    FROM {{ ref('f_applications_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_br AS (
    SELECT *
    FROM {{ ref('f_applications_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_loan_proposals_co AS (
    SELECT *
    FROM {{ ref('f_loan_proposals_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_loan_proposals_br AS (
    SELECT *
    FROM {{ ref('f_loan_proposals_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_underwriting_fraud_stage_co AS (
    SELECT *
    FROM {{ ref('f_underwriting_fraud_stage_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_underwriting_fraud_stage_br AS (
    SELECT *
    FROM {{ ref('f_underwriting_fraud_stage_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_allies_product_policies_co AS (
    SELECT *
    FROM {{ ref('f_allies_product_policies_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_approval_loans_to_refinance_co AS (
    SELECT *
    FROM {{ ref('f_approval_loans_to_refinance_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
d_fx_rate AS (
    SELECT *
    FROM {{ source('silver', 'd_fx_rate') }}
)
,
d_fng_fee AS (
    SELECT *
    FROM {{ source('risk', 'fng_fee') }}
)
,
ds_expected_collection_fees_results AS (
    SELECT *
    FROM {{ source('silver', 'ds_expected_collection_fees_results') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_ally_brand_ally_slug_status AS (
    SELECT *
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
)
,
bl_originations_payment_date_co AS (
    SELECT *
    FROM {{ ref('bl_originations_payment_date_co') }}
)
,
bl_originations_marketplace_suborders_to_originations_co AS (
    SELECT *
    FROM {{ ref('bl_originations_marketplace_suborders_to_originations_co') }}
)
,
bl_application_addi_shop_co AS (
    SELECT *
    FROM {{ ref('bl_application_addi_shop_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
       OR application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif %}
)
,
bl_application_channel AS (
    SELECT *
    FROM {{ ref('bl_application_channel') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
       OR application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
-- JOINING BOTH CO & BR
bl_application_product AS (
    (
    SELECT application_id,
           original_product,
           processed_product,
           synthetic_product_category,
           synthetic_product_subcategory
    FROM {{ ref('bl_application_product_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif %}
    )
    UNION ALL
    (
    SELECT application_id,
           original_product,
           processed_product,
           synthetic_product_category,
           synthetic_product_subcategory
    FROM {{ ref('bl_application_product_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif %}
    )
)
,
br_bad_book_clients AS (
    (SELECT client_id FROM {{ source('risk', 'seg1_cut_br_v2') }})
    UNION ALL
    (SELECT client_id FROM {{ source('risk', 'seg2_cut_br_v2') }})
)
,
-- JOINING BOTH CO & BR RMT
risk_master_tables AS (
    (
    SELECT
        loan_id,
        CASE
            WHEN dpd_plus_1_month>1 THEN upb_plus_1_month
            WHEN dpd_plus_1_month<=1 THEN 0
            ELSE NULL
        END AS dq31_at_31_upb,
        CASE
            WHEN dpd_plus_1_month IS NOT NULL THEN approved_amount
        END AS dq31_at_31_opb,
        fp_date_plus_1_month AS dq31_at_31_date
    FROM {{ ref('risk_master_table_co') }}
    WHERE loan_id IS NOT NULL
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif %}
    )
    UNION ALL
    (
    SELECT
        loan_id,
        CASE
            WHEN dpd_plus_1_month>1 THEN upb_plus_1_month
            WHEN dpd_plus_1_month<=1 THEN 0
            ELSE NULL END AS dq31_at_31_upb,
        CASE
            WHEN dpd_plus_1_month IS NOT NULL THEN general_amount
        END AS dq31_at_31_opb,
        fp_date_plus_1_month AS dq31_at_31_date
    FROM {{ ref('risk_master_table_br') }}
    WHERE loan_id IS NOT NULL
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif %}
    )
)
,
-- //* CO LOSSES CALCULATIONS SECTION *//
--Context: https://addico.slack.com/archives/C01K51BLLEB/p1713455679361069
--Including also fix for the following incident:
--https://addico.slack.com/archives/C05F4587X27/p1688162477828489
grande_true_up_losses AS (
    SELECT gtulf.loan_id,
           --expected_dollar_losses is expressed in COP
           CASE WHEN a.client_type = 'PROSPECT' THEN gtulf.expected_dollar_losses
                WHEN a.client_type = 'CLIENT'   THEN o.approved_amount * 0.07
                END AS pred_co_bal
    FROM {{ source('silver', 'grande_true_up_loss_forecast') }} AS gtulf
    INNER JOIN f_originations_bnpl_co AS o ON o.loan_id = gtulf.loan_id
    INNER JOIN f_applications_co      AS a ON a.application_id = o.application_id
)
,
expected_losses_co AS (
    (
    SELECT
        loan_id,
        co_factor,
        prediction_31_31_unit,
        NULL AS pred_co_bal,
        FALSE AS is_grande_risk_product_category
    FROM {{ source('risk', 'prospect_loss_forecast') }}
    )
    UNION ALL
    (
    SELECT
        loan_id,
        co_factor,
        prediction_31_31_unit_adjusted AS prediction_31_31_unit,
        NULL AS pred_co_bal,
        FALSE AS is_grande_risk_product_category
    FROM {{ source('risk', 'client_loss_forecast') }}
    )
    UNION ALL
    (
    --Compra Grande Expected Losses
    SELECT
        COALESCE(glf.loan_id, gtul.loan_id) AS loan_id,
        NULL AS co_factor,
        NULL AS prediction_31_31_unit,
        COALESCE(glf.net_expected_lifetime_losses,
                 gtul.pred_co_bal) AS pred_co_bal,
        TRUE AS is_grande_risk_product_category
    FROM {{ source('silver', 'grande_loss_forecast') }} AS glf
    FULL OUTER JOIN grande_true_up_losses gtul ON glf.loan_id = gtul.loan_id
    )
)
,
-- //* CO & BR APPLICATIONS SECTION *//
f_applications_backfill AS (
    (
    SELECT
        application_id,
        FIRST_VALUE(ally_slug,TRUE) AS ally_slug,
        FIRST_VALUE(channel,TRUE) AS channel,
        FIRST_VALUE(client_id,TRUE) AS client_id,
        FIRST_VALUE(client_type,TRUE) AS client_type,
        FIRST_VALUE(journey_name,TRUE) AS journey_name
    FROM {{ ref('f_origination_events_co_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {% endif %}
    GROUP BY 1
    )
    UNION ALL
    (
    SELECT
        application_id,
        FIRST_VALUE(ally_slug,TRUE) AS ally_slug,
        FIRST_VALUE(channel,TRUE) AS channel,
        FIRST_VALUE(client_id,TRUE) AS client_id,
        FIRST_VALUE(client_type,TRUE) AS client_type,
        FIRST_VALUE(journey_name,TRUE) AS journey_name
    FROM {{ ref('f_origination_events_br_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {% endif %}
    GROUP BY 1
    )
)
,
f_applications AS (
    SELECT
        -- BACKFIELD FIELDS
        COALESCE(a.application_id, ab.application_id) AS application_id,
        COALESCE(a.ally_slug, ab.ally_slug) AS ally_slug,
        COALESCE(a.channel, ab.channel, a.application_channel_legacy) AS channel,
        COALESCE(a.client_id, ab.client_id) AS client_id,
        COALESCE(a.journey_name, ab.journey_name) AS journey_name,
        -- JUST-AS-IS FIELDS
        a.order_id,
        a.requested_amount_without_discount,
        a.requested_amount,
        a.custom_is_santander_branched,
        a.store_slug,
        -- CALCULATED FIELDS
        CASE
            WHEN COALESCE(a.client_type, ab.client_type) ILIKE '%client%' OR a.journey_name ILIKE '%client%' OR a.custom_is_returning_client_legacy THEN 'CLIENT'
            ELSE 'PROSPECT'
        END AS client_type,
        CASE
            WHEN a.requested_amount_without_discount >0 THEN a.requested_amount_without_discount
            ELSE a.requested_amount
        END AS synthetic_requested_amount,
        CASE WHEN a.suborders_ally_slug_array IS NOT NULL THEN IF(SIZE(a.suborders_ally_slug_array)>0, SORT_ARRAY(a.suborders_ally_slug_array),NULL) END AS suborders_ally_slug_array
    FROM
    ( -- UNIONing CO AND BR
        ( -- CO
        SELECT
            application_id,
            ally_slug,
            channel,
            application_channel_legacy,
            client_id,
            client_type,
            custom_is_returning_client_legacy,
            journey_name,
            order_id,
            requested_amount_without_discount,
            requested_amount,
            custom_is_santander_branched,
            suborders_ally_slug_array,
            store_slug
        FROM f_applications_co
        )
        UNION ALL
        ( -- BR
        SELECT
            application_id,
            ally_slug,
            channel,
            application_channel_legacy,
            client_id,
            client_type,
            custom_is_returning_client_legacy,
            journey_name,
            order_id,
            requested_amount_without_discount,
            requested_amount,
            FALSE AS custom_is_santander_branched,
            NULL AS suborders_ally_slug_array,
            store_slug
        FROM f_applications_br
        )
    ) AS a
    FULL OUTER JOIN f_applications_backfill AS ab ON ab.application_id = a.application_id
)
,
-- //* CO & BR ORIGINATIONS SECTION *//
f_originations AS (
    ( -- CO BNPL ORIGINATIONS
    SELECT
        "CO" AS country_code,
        -- ORIGINATIONS FIELDS
        o.application_id,
        o.loan_id,
        o.client_id,
        o.ally_slug,
        o.approved_amount,
        COALESCE(o.custom_is_santander_originated, FALSE) AS custom_is_santander_originated,
        o.guarantee_rate::DOUBLE AS guarantee_rate,
        CASE WHEN o.guarantee_provider IS NOT NULL THEN o.guarantee_provider
             WHEN o.guarantee_provider IS NULL AND o.guarantee_rate > 0.0 THEN 'FGA'
             ELSE NULL END AS guarantee_provider_with_default,
        o.lbl,
        o.origination_date,
        from_utc_timestamp(o.origination_date,"America/Bogota") AS origination_date_local,
        o.term,
        -- LOAN PROPOSALS FIELDS
        CASE WHEN l.ally_mdf::DOUBLE < 1 THEN l.ally_mdf::DOUBLE ELSE (l.ally_mdf::DOUBLE)/100 END AS ally_mdf, -- Fix to 1500 LPs with data quality issues from events
        l.interest_rate::DOUBLE AS interest_rate,
        l.total_interest::DOUBLE AS total_interest,
        COALESCE(l.fga_tax_rate::DOUBLE, 0.19) AS fga_tax_rate,
        -- UNDERWRITING FIELDS
        u.credit_policy_name,
        u.pd_calculation_method,
        -- OTHER FIELDS
        altr.refinanced_by_origination_of_loan_id,
        COALESCE(ecf.collection_fee_income_amount, 0)::DOUBLE AS collection_fee_income_amount
    FROM      f_originations_bnpl_co              AS o
    LEFT JOIN f_loan_proposals_co                 AS l    ON o.loan_id        = l.loan_proposal_id
    LEFT JOIN f_underwriting_fraud_stage_co       AS u    ON o.application_id = u.application_id
    LEFT JOIN f_approval_loans_to_refinance_co    AS altr ON o.loan_id        = altr.loan_id
    LEFT JOIN ds_expected_collection_fees_results AS ecf  ON o.loan_id        = ecf.loan_id
    WHERE o.ally_slug != 'addi-preapprovals' --Son 9 originaciones de 2021 que vienen con este slug y preapproval as channel, las excluyo
    )
    UNION ALL
    ( -- CO BNPN ORIGINATIONS
    SELECT
        "CO" AS country_code,
        -- ORIGINATIONS FIELDS
        o.application_id,
        NULL::STRING AS loan_id,
        o.client_id,
        o.ally_slug,
        o.payment_amount AS approved_amount,
        FALSE AS custom_is_santander_originated,
        NULL::DOUBLE AS guarantee_rate,
        NULL:STRING AS guarantee_provider_with_default,
        NULL::BOOLEAN AS lbl,
        o.last_event_ocurred_on_processed AS origination_date,
        from_utc_timestamp(o.last_event_ocurred_on_processed,"America/Bogota") AS origination_date_local,
        NULL::INTEGER AS term,
        -- LOAN PROPOSALS FIELDS
        apol.origination_mdf::DOUBLE AS ally_mdf,
        NULL::DOUBLE AS interest_rate,
        NULL::DOUBLE AS total_interest,
        NULL::DOUBLE AS fga_tax_rate,
        -- UNDERWRITING FIELDS
        NULL::STRING AS credit_policy_name,
        NULL::STRING AS pd_calculation_method,
        -- OTHER FIELDS
        NULL::STRING AS refinanced_by_origination_of_loan_id,
        NULL::DOUBLE AS collection_fee_income_amount
    FROM      f_originations_bnpn_co       AS o
    LEFT JOIN f_allies_product_policies_co AS apol ON o.application_id = apol.application_id AND apol.type = 'ADDI_BNPN' and apol.product = 'BNPN_CO'
    )
    UNION ALL
    ( -- BR BNPL ORIGINATIONS
    SELECT
        "BR" AS country_code,
        -- ORIGINATIONS FIELDS
        o.application_id,
        o.loan_id,
        o.client_id,
        o.ally_slug,
        o.approved_amount,
        FALSE AS custom_is_santander_originated,
        NULL::DOUBLE AS guarantee_rate,
        NULL:STRING AS guarantee_provider_with_default,
        o.lbl,
        o.origination_date,
        from_utc_timestamp(o.origination_date,"America/Sao_Paulo") AS origination_date_local,
        o.term,
        -- LOAN PROPOSALS FIELDS
        l.ally_mdf::DOUBLE AS ally_mdf,
        l.interest_rate::DOUBLE AS interest_rate,
        l.total_interest::DOUBLE AS total_interest,
        NULL::DOUBLE AS fga_tax_rate,
        -- UNDERWRITING FIELDS
        u.credit_policy_name,
        u.pd_calculation_method,
        -- OTHER FIELDS
        NULL::STRING AS refinanced_by_origination_of_loan_id,
        NULL::DOUBLE AS collection_fee_income_amount
    FROM      f_originations_bnpl_br        AS o
    LEFT JOIN f_loan_proposals_br           AS l ON o.loan_id        = l.loan_proposal_id
    LEFT JOIN f_underwriting_fraud_stage_br AS u ON o.application_id = u.application_id
    )
    UNION ALL
    ( -- CO BNPN ORIGINATIONS
    SELECT
        "BR" AS country_code,
        -- ORIGINATIONS FIELDS
        o.application_id,
        NULL::STRING AS loan_id,
        o.client_id,
        o.ally_slug,
        o.requested_amount AS approved_amount,
        FALSE AS custom_is_santander_originated,
        NULL::DOUBLE AS guarantee_rate,
        NULL:STRING AS guarantee_provider_with_default,
        NULL::BOOLEAN AS lbl,
        o.origination_date,
        from_utc_timestamp(o.origination_date,"America/Sao_Paulo") AS origination_date_local,
        NULL::INTEGER AS term,
        -- LOAN PROPOSALS FIELDS
        NULL::DOUBLE AS ally_mdf,
        NULL::DOUBLE AS interest_rate,
        NULL::DOUBLE AS total_interest,
        NULL::DOUBLE AS fga_tax_rate,
        -- UNDERWRITING FIELDS
        NULL::STRING AS credit_policy_name,
        NULL::STRING AS pd_calculation_method,
        -- OTHER FIELDS
        NULL::STRING AS refinanced_by_origination_of_loan_id,
        NULL::DOUBLE AS collection_fee_income_amount
    FROM      f_originations_bnpn_br AS o
    )
 )
 ,
 -- //* CO & BR ORIGINATIONS CUSTOM BUSINESS LOGIC SECTION *//
dm_originations_baseline AS (
 --STEP 5--
    SELECT
       *, --To avoid redundancy, only new fields
       (merchant_revenue + consumer_revenue)::DOUBLE AS total_revenue
    FROM
    ( --STEP 4--
        SELECT
            *, --To avoid redundancy, only new fields
            CASE WHEN guarantee_provider_with_default = 'FGA' THEN TRUE ELSE FALSE END AS has_fga_flag,
            (   COALESCE(synthetic_origination_marketplace_purchase_fee_amount,0)
                + COALESCE(synthetic_lead_gen_fee_amount,0)
                + COALESCE(synthetic_ally_mdf_amount,0)
             )::DOUBLE AS merchant_revenue,
            (   COALESCE(synthetic_total_interest_post_losses,0)
                + COALESCE(collection_fee_income_amount,0)
            )::DOUBLE AS consumer_revenue,
            expected_final_losses_amount - synthetic_guarantee_expected_loss_recovery_amount AS expected_final_losses
        FROM
        ( --STEP 3--
            SELECT
                *, --To avoid redundancy, only new fields
                (CASE
                    WHEN guarantee_provider_with_default = 'FNG' THEN synthetic_guarantee_amount * (1 - synthetic_expected_loss_rate/2) 
                    WHEN synthetic_product_category = 'GRANDE' THEN synthetic_guarantee_amount * (1/1.19) * 0.9 * (1 - synthetic_expected_loss_rate/2)
                    ELSE synthetic_guarantee_amount * (1/1.19) * 0.9 * (1 - synthetic_expected_loss_rate)
                END)::DOUBLE AS synthetic_guarantee_amount_post_losses_tax_fee,
                (CASE
                    WHEN synthetic_product_category = 'GRANDE' THEN synthetic_total_interest_non_santander * (1 - synthetic_expected_loss_rate/2)
                    ELSE synthetic_total_interest_non_santander * (1 - synthetic_expected_loss_rate)
                END)::DOUBLE AS synthetic_total_interest_post_losses,
                (CASE
                    WHEN guarantee_provider_with_default = 'FNG' THEN expected_final_losses_amount * 0.9
                    ELSE 0 END)::DOUBLE AS synthetic_guarantee_expected_loss_recovery_amount
            FROM
            ( --STEP 2--
                SELECT
                    *, --To avoid redundancy, only new fields
                    COALESCE(synthetic_origination_origination_mdf, ally_mdf)::DOUBLE AS synthetic_ally_mdf, -- From pre-calculation in marketplace, else normal source
                    COALESCE(synthetic_origination_origination_mdf_amount, ally_mdf_amount)::DOUBLE AS synthetic_ally_mdf_amount, -- From pre-calculation in marketplace, else normal source
                    (approved_amount * COALESCE(guarantee_rate,0))::DOUBLE AS synthetic_guarantee_amount,
                    (synthetic_requested_amount_non_santander * COALESCE(lead_gen_fee_rate,0))::DOUBLE AS synthetic_lead_gen_fee_amount,
                    --synthetic_total_interest_post_losses -- BUILT ON STEP 3 due to needing `synthetic_expected_loss_rate`
                    (CASE
                        WHEN expected_final_losses_amount IS NOT NULL THEN approved_amount
                        ELSE 0
                    END)::DOUBLE AS approved_amount_filtered_for_losses,
                    (CASE WHEN country_code = 'CO' THEN COALESCE(expected_final_losses_amount,0) / approved_amount
                        ELSE 0
                    END)::DOUBLE AS synthetic_expected_loss_rate
                FROM
                ( --STEP 1--
                    SELECT
                        -- A. SOURCE FIELDS - f_originations
                        o.country_code,
                        o.application_id,
                        o.loan_id,
                        o.client_id,
                        o.ally_slug,
                        o.approved_amount,
                        o.custom_is_santander_originated,
                        o.guarantee_rate,
                        o.guarantee_provider_with_default,
                        o.lbl,
                        o.origination_date,
                        o.origination_date_local,
                        o.term,
                        o.ally_mdf,
                        o.interest_rate,
                        o.total_interest,
                        o.credit_policy_name,
                        o.pd_calculation_method,
                        o.refinanced_by_origination_of_loan_id,
                        o.collection_fee_income_amount,
                        -- B. ADDITIONAL CUSTOM BUSINESS-LOGIC FIELDS - Columns from other datasets and custom business logic
                        --  -- Marketplace-only: sub-orders a posteriori calculations at the application-origination level
                        --  -- For context on why to retrieve amounts as well refer to the comment on the last select of the model: bl_originations_marketplace_suborders_to_originations_co
                        mkplc_bl.synthetic_origination_marketplace_purchase_fee,
                        mkplc_bl.synthetic_origination_marketplace_purchase_fee_amount,
                        mkplc_bl.synthetic_origination_origination_mdf,
                        mkplc_bl.synthetic_origination_origination_mdf_amount,
                        --  -- Other fields
                        a.synthetic_requested_amount * COALESCE(o.ally_mdf,0) AS ally_mdf_amount,
                        oas.lead_gen_fee_rate::DOUBLE AS lead_gen_fee_rate,
                        ap.synthetic_product_category,
                        a.synthetic_requested_amount,
                        COALESCE(asl.ally_cluster,'KA') AS ally_cluster_with_default,
                        (CASE
                            WHEN COALESCE(elco.is_grande_risk_product_category,FALSE) = FALSE THEN (elco.prediction_31_31_unit * o.approved_amount) * elco.co_factor
                            WHEN COALESCE(elco.is_grande_risk_product_category,FALSE) = TRUE THEN elco.pred_co_bal
                        END)::DOUBLE AS expected_final_losses_amount,
                        CASE
                            WHEN bb_c.client_id IS NULL AND o.country_code='BR' THEN 'good_book'
                            WHEN bb_c.client_id IS NOT NULL AND o.country_code='BR' THEN 'bad_book'
                        END AS client_segment_br,
                        (CASE
                            WHEN o.custom_is_santander_originated IS NOT TRUE THEN a.synthetic_requested_amount
                            ELSE 0
                        END)::DOUBLE AS synthetic_requested_amount_non_santander,
                        (CASE WHEN o.custom_is_santander_originated IS NOT TRUE THEN o.total_interest
                            ELSE 0
                        END)::DOUBLE AS synthetic_total_interest_non_santander,
                        CASE
                            WHEN opd.report_term = 'WEEKLY' THEN 4
                            WHEN opd.report_term = 'MONTHLY' THEN 15
                            ELSE datediff(opd.payment_date,o.origination_date)
                        END AS ally_diff_payment_date,
                        CASE
                            WHEN o.guarantee_provider_with_default = 'FNG' THEN (o.approved_amount * ff.fng_cost_rate) + (o.approved_amount * (guarantee_rate * ((1/(1+fga_tax_rate)) * fga_tax_rate)))
                            ELSE NULL END AS synthetic_fng_cost_amount 
                    FROM      f_originations                                           AS o 
                    LEFT JOIN f_applications                                           AS a        ON o.application_id = a.application_id
                    LEFT JOIN bl_application_product                                   AS ap       ON o.application_id = ap.application_id
                    LEFT JOIN bl_originations_marketplace_suborders_to_originations_co AS mkplc_bl ON o.application_id = mkplc_bl.application_id
                    LEFT JOIN bl_application_addi_shop_co                                 AS oas      ON o.application_id = oas.application_id AND a.channel != 'ADDI_MARKETPLACE' --FORCE MKTPLC OUT
                    LEFT JOIN bl_ally_brand_ally_slug_status                           AS asl      ON o.ally_slug      = asl.ally_slug AND asl.country_code = o.country_code
                    LEFT JOIN bl_originations_payment_date_co                          AS opd      ON o.loan_id        = opd.loan_id
                    LEFT JOIN expected_losses_co                                       AS elco     ON o.loan_id        = elco.loan_id
                    LEFT JOIN br_bad_book_clients                                      AS bb_c     ON o.client_id      = bb_c.client_id AND o.country_code = 'BR'
                    LEFT JOIN d_fng_fee                                                AS ff       ON o.term           = ff.term AND current_date() BETWEEN ff.start_valid_period AND ff.end_valid_period
                ) AS s1 --STEP 1--
            ) AS s2 --STEP 2--
        ) AS s3 --STEP 3--
    ) AS s4 --STEP 4--
 --STEP 5--
)

-- //* FINAL SELECT - NO BUSINESS LOGIC, JUST RETRIEVING AND RENAMING FIELDS *//
SELECT
    -- A. KEY FIELDS
    o.country_code,
    o.application_id,
    o.loan_id,
    COALESCE(o.client_id, a.client_id) AS client_id,
    o.refinanced_by_origination_of_loan_id,
    a.order_id,
    COALESCE(o.ally_slug, a.ally_slug) AS ally_slug,
    -- B. ORIGINATIONS DATA
    a.store_slug,
    a.suborders_ally_slug_array,
    o.origination_date,
    o.origination_date_local,
    HOUR(o.origination_date_local) AS origination_hour_local,
    MINUTE(o.origination_date_local) AS origination_minute_local,
    o.lbl,
    o.term,
    o.credit_policy_name,
    o.pd_calculation_method,
    -- C. APPLICATIONS FIELDS
    a.client_type,
    a.journey_name,
    a.channel AS application_channel,
    ac.synthetic_channel,
    ap.original_product,
    ap.processed_product,
    ap.synthetic_product_category,
    ap.synthetic_product_subcategory,
    a.custom_is_santander_branched AS santander_branched,
    o.custom_is_santander_originated AS santander_origination,
    -- D. AMOUNTS AND RATES - REVENUE METRICS
    o.approved_amount,
    a.synthetic_requested_amount AS requested_amount,
    a.synthetic_requested_amount AS gmv,
    o.guarantee_rate,
    o.guarantee_provider_with_default,
    o.synthetic_fng_cost_amount AS fng_cost_amount,
    o.synthetic_guarantee_amount AS guarantee_amount_charged_at_checkout,
    o.synthetic_guarantee_amount_post_losses_tax_fee AS guarantee_amount,
    o.has_fga_flag,
    o.interest_rate,
    o.synthetic_total_interest_post_losses AS total_interest,
    o.lead_gen_fee_rate,
    o.synthetic_lead_gen_fee_amount AS lead_gen_fee_amount,
    o.synthetic_ally_mdf AS ally_mdf,
    o.synthetic_ally_mdf_amount AS mdf_amount,
    o.synthetic_origination_marketplace_purchase_fee,
    o.synthetic_origination_marketplace_purchase_fee_amount,
    o.collection_fee_income_amount AS expected_collection_fee_amount,
    o.consumer_revenue,
    o.merchant_revenue,
    o.total_revenue,
    -- E. ALLY DATA
    asl.ally_vertical,
    asl.ally_brand,
    o.ally_cluster_with_default AS ally_cluster,
    -- F. LOAN PERFORMANCE DATA + LOSSES FIELDS
    o.approved_amount_filtered_for_losses,
    o.synthetic_guarantee_expected_loss_recovery_amount AS guarantee_expected_loss_recovery_amount,
    o.expected_final_losses_amount AS gross_expected_final_losses_amount,
    o.expected_final_losses,
    o.synthetic_expected_loss_rate AS expected_final_losses_rate,
    rmt.dq31_at_31_upb,
    rmt.dq31_at_31_opb,
    rmt.dq31_at_31_date,
    -- G. ADDI SHOP REFERRAL DATA
    oas.is_addishop_referral,
    oas.is_addishop_referral_paid,
    oas.addishop_channel,
    oas.used_grouped_config AS shop_used_grouped_config,
    oas.addi_shop_ally_period_opt_in_date AS addishop_opt_in_date,
    oas.addi_shop_ally_period_opt_out_date AS addishop_opt_out_date,
    -- H. ALLY PAYMENT DATA
    opd.payment_date AS loan_payment_date,
    opd.report_term AS ally_report_term,
    opd.payment_term AS ally_payment_term,
    o.ally_diff_payment_date,
    -- I. OTHER FIELDS
    fr.price AS fx_rate,
    o.client_segment_br AS segment,
    -- J. DATA PLATFORM DATA
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM      dm_originations_baseline          AS o
LEFT JOIN f_applications                    AS a   ON o.application_id = a.application_id
LEFT JOIN d_fx_rate                         AS fr  ON o.country_code   = fr.country_code AND fr.is_active = TRUE
LEFT JOIN risk_master_tables                AS rmt ON o.loan_id        = rmt.loan_id
LEFT JOIN bl_application_product            AS ap  ON o.application_id = ap.application_id
LEFT JOIN bl_application_addi_shop_co       AS oas ON o.application_id = oas.application_id AND a.channel != 'ADDI_MARKETPLACE' --FORCE MKTPLC OUT
LEFT JOIN bl_application_channel            AS ac  ON o.application_id = ac.application_id
LEFT JOIN bl_originations_payment_date_co   AS opd ON o.loan_id        = opd.loan_id
LEFT JOIN bl_ally_brand_ally_slug_status    AS asl ON o.ally_slug      = asl.ally_slug AND asl.country_code = o.country_code
