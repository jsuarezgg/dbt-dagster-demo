{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH d_ally_management_store_users_co AS (
    SELECT
        user_id,
        email,
        INITCAP(TRIM(CONCAT(COALESCE(first_name,''),' ',COALESCE(middle_name,''),' ',COALESCE(last_name,''),' ',COALESCE(second_last_name,'')))) AS custom_full_name
    FROM {{ ref('d_ally_management_store_users_co') }}
)
,
f_syc_originations_co AS (
    SELECT *
    FROM {{ ref('f_syc_originations_co') }}
)
,
f_originations_bnpl_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_originations_bnpl_co') }}
)
,
f_applications_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_co') }}
)
,
f_ally_management_transactions_co AS (
    SELECT *
    FROM {{ ref('f_ally_management_transactions_co') }}
)
,
f_client_management_loan_cancellation_orders_co AS (
    SELECT *
    FROM {{ ref('f_client_management_loan_cancellation_orders_co') }}
)
,
f_loan_cancellations_v2_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_loan_cancellations_v2_co') }}
)
,
f_loan_cancellations_v2_co_logs AS (
    SELECT *
    FROM {{ source('silver_live', 'f_loan_cancellations_v2_co_logs') }}
)
,
loan_cancellations_v2_logs_partial_cancellations_marketplace_allies AS (
    SELECT
        loan_cancellation_id,
        marketplace_suborder_ally,
        loan_cancellation_amount AS cancellation_amount
    FROM f_loan_cancellations_v2_co_logs
    WHERE
        marketplace_suborder_ally IS NOT NULL --Marketplace only
        AND event_name = 'LoanCancellationOrderProcessedV2' AND loan_cancellation_type = 'PARTIAL' --ONLY PARTIAL ONES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_cancellation_id ORDER BY loan_cancellation_order_date) = 1
)
,
f_ally_management_transactions_co_grouped_by_loan_id AS (
    SELECT
        loan_id,
        transaction_id,
        COUNT(1) AS debug_ally_management_transactions_num_allies,
        COLLECT_LIST(ally_slug) AS debug_ally_management_transactions_allies_array,
        COUNT(1) FILTER (WHERE status = 'PARTIAL_CANCELED' /*AND cancellation_status = 'FINISHED'*/ ) AS debug_ally_management_transactions_num_partial_cancellations,
        COUNT(1) FILTER (WHERE status = 'CANCELED' /*AND cancellation_status = 'FINISHED'*/ ) AS debug_ally_management_transactions_num_total_cancellations,
        COLLECT_LIST(
            STRUCT(
                ally_slug,
                status,
                cancellation_id,
                cancellation_reason,
                cancellation_requested_at,
                cancellation_created_at,
                cancellation_status,
                cancellation_source,
                cancellation_user_name
            )
        ) AS debug_ally_management_transactions_data
    FROM f_ally_management_transactions_co
    WHERE loan_id IS NOT NULL /*AND cancellation_id IS NOT null*/
    GROUP BY 1,2
)
,
loan_cancellation_orders_co_baseline AS (
    SELECT
        lco.loan_cancellation_id,
        lco.data_source,
        lco.loan_id,
        lco.ally_slug,
        lco.marketplace_suborder_ally,
        lco.client_id,
        lco.user_id,
        su.custom_full_name AS user_custom_full_name,
        su.email AS user_email,
        lco.origination_date,
        lco.origination_channel,
        lco.approved_amount,
        COALESCE(lco.cancellation_type,'TOTAL_BACKFILL') AS cancellation_type,
        lco.cancellation_date,
        lco.cancellation_amount,
        lco.cancellation_reason,
        lco.is_annulled,
        lco.annulment_reason/*,
        lco.event_name,
        lco.event_timestamp*/
    FROM
    ( -- A.+ B. Cancellation data
        (-- A. f_client_management_loan_cancellation_orders_co
        SELECT
            cm_lco.loan_cancellation_id,
            'direct_modeling_client_management_loan_cancellation_orders_co' AS data_source,
            o.ally_slug,
            logs_pc.marketplace_suborder_ally,
            cm_lco.loan_id,
            cm_lco.client_id,
            cm_lco.user_id,
            cm_lco.origination_date,
            cm_lco.origination_channel,
            cm_lco.approved_amount,
            cm_lco.cancellation_type,
            cm_lco.cancellation_date,
            cm_lco.cancellation_amount,
            cm_lco.cancellation_reason,
            cm_lco.is_annulled,
            cm_lco.annulment_reason/*,
            event_name,
            event_timestamp*/
        FROM       f_client_management_loan_cancellation_orders_co                      AS cm_lco
        LEFT JOIN  loan_cancellations_v2_logs_partial_cancellations_marketplace_allies  AS logs_pc ON cm_lco.loan_cancellation_id = logs_pc.loan_cancellation_id
        LEFT JOIN  f_originations_bnpl_co                                               AS o       ON cm_lco.loan_id = o.loan_id
        )
        UNION ALL
        ( -- B. f_loan_cancellations_v2_co -- ONLY ADDING MISSING (COMPARED TO THE TRANSACTIONAL TABLE) NON-ANNULLED ONES
        SELECT
            loan_cancellation_id,
            'events_modeling_client_management_domain_events' AS data_source,
            ally_slug,
            marketplace_suborder_ally,
            loan_id,
            client_id,
            user_id,
            loan_origination_date AS origination_date,
            NULL AS origination_channel,
            loan_approved_amount AS approved_amount,
            loan_cancellation_type AS cancellation_type,
            loan_cancellation_order_date AS cancellation_date,
            loan_cancellation_amount AS cancellation_amount,
            loan_cancellation_reason AS cancellation_reason,
            loan_cancellation_annulment_reason IS NOT NULL AS is_annulled,
            loan_cancellation_annulment_reason AS annulment_reason/*,
            last_event_name_processed AS event_name,
            last_event_ocurred_on_processed AS event_timestamp*/
        FROM f_loan_cancellations_v2_co
        WHERE
            last_event_ocurred_on_processed < NOW()- INTERVAL '12' HOUR --To ignore records pending dms replication in directly modeled table
            AND last_event_name_processed != 'LoanCancellationOrderAnnulledV2'
            AND loan_id NOT IN (SELECT DISTINCT loan_id FROM f_client_management_loan_cancellation_orders_co)
        )
    ) AS lco -- Loan Cancellation Orders
    -- Getting the user data
    LEFT JOIN d_ally_management_store_users_co AS su ON lco.user_id = su.user_id
)
,
loan_cancellation_orders_co_total_cancellations AS (
    -- TOTAL CANCELLATIONS -
    -- STEP 2--
    SELECT
        *
    FROM
    ( -- STEP 1--
        SELECT
            loan_id,
            TRUE AS custom_is_totally_cancelled,
            COUNT(1) OVER (PARTITION BY loan_id) AS num_non_annulled_total_cancellations,
            loan_cancellation_id AS last_total_cancellation_request_id,
            user_id AS last_total_cancellation_request_user_id,
            user_custom_full_name AS last_total_cancellation_request_user_custom_full_name,
            cancellation_type AS last_total_cancellation_type,
            cancellation_date AS last_total_cancellation_date,
            cancellation_reason AS last_total_cancellation_reason/*,
            event_name AS last_total_cancellation_event_name,
            event_timestamp AS last_total_cancellation_event_timestamp*/
        FROM loan_cancellation_orders_co_baseline
        WHERE COALESCE(cancellation_type,'TOTAL_BACKFILL') IN ('TOTAL','TOTAL_BACKFILL') AND is_annulled = FALSE
    ) AS s1 -- STEP 1
    -- Deduplicate in case multiple non-annulled total cancellations
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY last_total_cancellation_date DESC) = 1
    -- STEP 2 --
)
,
loan_cancellation_orders_co_partial_cancellations AS (
    -- PARTIAL CANCELLATIONS -
    -- STEP 2
    SELECT
        loan_id,
        FIRST_VALUE(TRUE) AS custom_has_partial_cancellations,
        SUM(sum_ally_non_annulled_partial_cancellation_amounts) AS sum_non_annulled_partial_cancellation_amounts,
        SUM(num_ally_non_annulled_unique_partial_cancellations) AS num_non_annulled_unique_partial_cancellations,
        COUNT(1) AS num_unique_allies_with_non_annulled_partial_cancellation,
        COLLECT_LIST(
            STRUCT(
                partial_cancellation_ally_slug,
                sum_ally_non_annulled_partial_cancellation_amounts,
                num_ally_non_annulled_unique_partial_cancellations
            )
        ) AS debug_stats_partial_cancellations,
        COLLECT_LIST(
            STRUCT(
                partial_cancellation_ally_slug,
                debug_ally_partial_cancellations
        )) AS debug_detail_partial_cancellations
    FROM
    (-- STEP 1
        SELECT
            loan_id,
            COALESCE(marketplace_suborder_ally, ally_slug) AS partial_cancellation_ally_slug,
            SUM(cancellation_amount) AS sum_ally_non_annulled_partial_cancellation_amounts,
            count(1) AS num_ally_non_annulled_unique_partial_cancellations,
            COLLECT_LIST(
                STRUCT(
                    loan_cancellation_id,
                    ally_slug,
                    marketplace_suborder_ally,
                    cancellation_date,
                    cancellation_amount,
                    cancellation_reason,
                    user_custom_full_name,
                    user_id/*,
                    event_name,
                    event_timestamp*/
                )
            ) AS debug_ally_partial_cancellations
        FROM       loan_cancellation_orders_co_baseline
        WHERE cancellation_type IN ('PARTIAL') AND is_annulled = FALSE
        GROUP BY 1,2
    ) AS s1 -- STEP 1--
    GROUP BY 1
    -- STEP 2 --
)
,
loan_cancellation_orders_co_all_cancellations AS (
    -- ALL CANCELLATION ORDERS --
    -- STEP 2 --
    SELECT
        loan_id,
        COLLECT_LIST(
            STRUCT(
                custom_cancellation_order_index,
                loan_cancellation_id,
                ally_slug,
                marketplace_suborder_ally,
                cancellation_type,
                cancellation_date,
                cancellation_amount,
                cancellation_reason,
                is_annulled,
                annulment_reason,
                user_custom_full_name,
                user_id/*,
                event_name,
                event_timestamp*/
            )
        ) AS debug_all_loan_cancellations_orders_history,
        COUNT(1) AS debug_all_loan_cancellations_orders_num_orders
    FROM
    (-- STEP 1
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY cancellation_date) AS custom_cancellation_order_index
        FROM       loan_cancellation_orders_co_baseline
    ) AS s1 -- STEP 1--
    GROUP BY 1
    -- STEP 2 --
)
-- Data mart based only on loans with some sort of loan cancellation order in Client Management Loan Cancellation Orders,
-- lot of debug columns from this source for both total and  partial cancellations and from other other domains'
-- cancellation data sources.
SELECT
    --LOAN DATA
    COALESCE(t.loan_id, p.loan_id, all.loan_id) AS loan_id,
    COALESCE(o.client_id, a.client_id) AS client_id,
    COALESCE(o.application_id, a.application_id) AS application_id,
    o.ally_slug,
    CASE WHEN a.suborders_ally_slug_array IS NOT NULL THEN IF(SIZE(a.suborders_ally_slug_array)>0, SORT_ARRAY(a.suborders_ally_slug_array),NULL) END AS suborders_ally_slug_array,
    o.approved_amount,
    o.origination_date,
    from_utc_timestamp(o.origination_date,"America/Bogota") AS origination_date_cot,
    COALESCE(a.channel, a.application_channel_legacy) AS origination_channel,
    -- KEY BOOLEAN COLUMNS - Client Management Loan Cancellation Orders - Only consider non-annulled cancellations
    COALESCE(t.custom_is_totally_cancelled,FALSE) AS custom_is_totally_cancelled,
    COALESCE(p.custom_has_partial_cancellations,FALSE) AS custom_has_partial_cancellations,
    -- KEY LAST TOTAL CANCELLATION DATA - Client Management Loan Cancellation Orders - Only consider non-annulled cancellations
    t.last_total_cancellation_request_id,
    t.last_total_cancellation_request_user_id,
    t.last_total_cancellation_request_user_custom_full_name,
    from_utc_timestamp(t.last_total_cancellation_date,"America/Bogota") AS last_total_cancellation_date_cot,
    t.last_total_cancellation_type,
    t.last_total_cancellation_reason,
    COALESCE(t.num_non_annulled_total_cancellations,0) AS num_non_annulled_total_cancellations,
    --KEY PARTIAL CANCELLATION DATA - Client Management Loan Cancellation Orders - Only consider non-annulled cancellations
    COALESCE(p.sum_non_annulled_partial_cancellation_amounts,0) AS sum_non_annulled_partial_cancellation_amounts,
    COALESCE(p.num_non_annulled_unique_partial_cancellations,0) AS num_non_annulled_unique_partial_cancellations,
    COALESCE(p.num_unique_allies_with_non_annulled_partial_cancellation,0) AS num_unique_allies_with_non_annulled_partial_cancellation,
    p.debug_stats_partial_cancellations,
    p.debug_detail_partial_cancellations,
    --DEBUG CANCELLATION - Client Management Loan Cancellation Orders - Considers all cancellations
    all.debug_all_loan_cancellations_orders_num_orders,
    all.debug_all_loan_cancellations_orders_history,
    --DEBUG CANCELLATION - SYC schema
    COALESCE(so.is_cancelled, FALSE) AS debug_syc_originations_is_cancelled,
    so.cancellation_reason AS debug_syc_originations_cancellation_reason,
    --DEBUG CANCELLATION - ALLY MANAGEMENT schema
    COALESCE(am_t.debug_ally_management_transactions_num_allies,0) AS debug_ally_management_transactions_num_allies,
    am_t.debug_ally_management_transactions_allies_array,
    COALESCE(am_t.debug_ally_management_transactions_num_partial_cancellations,0)  AS debug_ally_management_transactions_num_partial_cancellations,
    COALESCE(am_t.debug_ally_management_transactions_num_total_cancellations,0)  AS debug_ally_management_transactions_num_total_cancellations,
    am_t.debug_ally_management_transactions_data,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM            loan_cancellation_orders_co_total_cancellations      AS t                             --t=total
FULL OUTER JOIN loan_cancellation_orders_co_partial_cancellations    AS p    ON t.loan_id = p.loan_id --p=partials
FULL OUTER JOIN loan_cancellation_orders_co_all_cancellations        AS all  ON COALESCE(t.loan_id, p.loan_id) = all.loan_id
LEFT JOIN       f_originations_bnpl_co                               AS o    ON COALESCE(t.loan_id, p.loan_id, all.loan_id) = o.loan_id
LEFT JOIN       f_applications_co                                    AS a    ON o.application_id = a.application_id
LEFT JOIN       f_syc_originations_co                                AS so   ON COALESCE(t.loan_id, p.loan_id, all.loan_id) = so.loan_id
LEFT JOIN       f_ally_management_transactions_co_grouped_by_loan_id AS am_t ON COALESCE(t.loan_id, p.loan_id, all.loan_id) = am_t.loan_id OR COALESCE(o.application_id, a.application_id) = am_t.transaction_id