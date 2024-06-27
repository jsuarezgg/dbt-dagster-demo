{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH
stg_funnel_step1_application_process_at_application_id_values_co_finished_only AS (
    SELECT *
    FROM {{ ref('stg_funnel_step1_application_process_at_application_id_values_co') }}
    WHERE has_finished
)
,
application_process_id_calculation_co AS (
    -- STEP 3: Use the custom_day_origination_ordinal in addition to the application_process_baseline_id to build
    --         the NEW application_process_id
    SELECT
        MD5(CONCAT(custom_day_origination_ordinal::STRING, application_process_baseline_id)) AS application_process_id,
        * --To avoid redundancy, only new fields
    FROM
    ( -- STEP 2: Modify the window function values in a way that groups non-approved applications up to an approved one
      -- Example:                       :  A= Application ; All from the same application_process_baseline_id
      --                                :  A1->A2(Approval)->A3->A4->A5(Approval)->A6->A7->A8|
      -- custom_day_origination_ordinal =  1               ->2                   ->3         |
        SELECT
            *, --To avoid redundancy, only new fields
            CASE WHEN has_reached_approval::int = 1 THEN window_has_reached_approval ELSE window_has_reached_approval + 1 END AS custom_day_origination_ordinal
        FROM
        ( -- STEP 1: Window function on applications approvals, needed to properly group applications in multi-approval scenarios for a single application_process_baseline_id
          -- Example:                    :  A= Application ; All from the same application_process_baseline_id
          --                             :  A1->A2(Approval)->A3->A4->A5(Approval)->A6->A7->A8|
          -- window_has_reached_approval =  0 ->1                    ->2                      |
            SELECT
                application_id,
                application_process_baseline_id,
                has_reached_approval,
                minimum_ocurred_on_local,
                approval_ocurred_on_local,
                synthetic_ocurred_on_local,
                SUM(has_reached_approval::int) OVER (PARTITION BY application_process_baseline_id ORDER BY synthetic_ocurred_on_local) AS window_has_reached_approval
            FROM stg_funnel_step1_application_process_at_application_id_values_co_finished_only
        ) -- STEP 1 --
    ) -- STEP 2 --
-- STEP 3 --
)
,
stg_funnel_step1_application_process_at_application_id_values_co_with_application_process_id AS (
    SELECT
        apc.application_process_id,
        apc.custom_day_origination_ordinal,
        -- apc.window_has_reached_approval, -- Not needed
        aiv.* --To avoid redundancy, only new fields
    FROM       stg_funnel_step1_application_process_at_application_id_values_co_finished_only AS aiv
    LEFT JOIN  application_process_id_calculation_co                                          AS apc ON aiv.application_id = apc.application_id
)

-- FINAL SELECT: All data at the (new) application_process_id level
SELECT
    -- PRIMARY KEY (SHOULD NEVER BE REPEATED)
    application_process_id,
    -- GROUP BY ALL VARIABLES CONSIDERED IN THE application_process_id (redundant but will create duplicates in the PK
    --                                                                  if something is wrong with our logic)
    application_process_baseline_id,
    custom_day_origination_ordinal,
    client_id,
    ally_slugs_string,
    synthetic_journey_name,
    synthetic_ocurred_on_date_local,
    -- KEY TIMESTAMP MIN AND MAX
    COALESCE(MAX(approval_ocurred_on_local), MIN(minimum_ocurred_on_local)) AS synthetic_ocurred_on_local,
    MAX(synthetic_ocurred_on_local) AS max_synthetic_ocurred_on_local,
    MIN(synthetic_ocurred_on_local) AS min_synthetic_ocurred_on_local,
    -- 'MAX VALUE' RESOLUTION VARIABLES
    MAX(has_reached_approval) AS has_reached_approval,
    MAX(is_using_preapproval_proxy) AS is_using_preapproval_proxy,
    -- 'FIRST VALUE' RESOLUTION VARIABLES
    FIRST_VALUE(journey_homolog, TRUE) AS journey_homolog,
    FIRST_VALUE(term, TRUE) AS term,
    FIRST_VALUE(ally_clusters_string, TRUE) AS ally_clusters_string,
    FIRST_VALUE(ally_brands_string, TRUE) AS ally_brands_string,
    FIRST_VALUE(ally_verticals_string, TRUE) AS ally_verticals_string,
    -- 'LAST NON-NULL VALUE' RESOLUTION VARIABLES - same approach as with silver events tables
    ELEMENT_AT(
        ARRAY_SORT(
            ARRAY_AGG(CASE WHEN client_type IS NOT NULL THEN STRUCT(synthetic_ocurred_on_local, client_type) ELSE NULL END),
                     (LEFT, RIGHT) -> CASE WHEN LEFT.synthetic_ocurred_on_local  < RIGHT.synthetic_ocurred_on_local THEN 1 WHEN LEFT.synthetic_ocurred_on_local  > RIGHT.synthetic_ocurred_on_local THEN -1 ELSE 0 END
            ),
        1
    ).client_type AS client_type,
    ELEMENT_AT(
        ARRAY_SORT(
            ARRAY_AGG(CASE WHEN channel IS NOT NULL THEN STRUCT(synthetic_ocurred_on_local, channel) ELSE NULL END),
                     (LEFT, RIGHT) -> CASE WHEN LEFT.synthetic_ocurred_on_local  < RIGHT.synthetic_ocurred_on_local THEN 1 WHEN LEFT.synthetic_ocurred_on_local  > RIGHT.synthetic_ocurred_on_local THEN -1 ELSE 0 END
            ),
        1
    ).channel AS channel,
    ELEMENT_AT(
        ARRAY_SORT(
            ARRAY_AGG(CASE WHEN synthetic_product_category IS NOT NULL THEN STRUCT(synthetic_ocurred_on_local, synthetic_product_category) ELSE NULL END),
                     (LEFT, RIGHT) -> CASE WHEN LEFT.synthetic_ocurred_on_local  < RIGHT.synthetic_ocurred_on_local THEN 1 WHEN LEFT.synthetic_ocurred_on_local  > RIGHT.synthetic_ocurred_on_local THEN -1 ELSE 0 END
            ),
        1
    ).synthetic_product_category AS synthetic_product_category,
    ELEMENT_AT(
        ARRAY_SORT(
            ARRAY_AGG(
                CASE WHEN synthetic_product_subcategory IS NOT NULL THEN STRUCT(synthetic_ocurred_on_local, synthetic_product_subcategory) ELSE NULL END),
                (LEFT, RIGHT) -> CASE  WHEN LEFT.synthetic_ocurred_on_local  < RIGHT.synthetic_ocurred_on_local THEN 1 WHEN LEFT.synthetic_ocurred_on_local  > RIGHT.synthetic_ocurred_on_local THEN -1 ELSE 0 END
            ),
        1
    ).synthetic_product_subcategory AS synthetic_product_subcategory,
    -- DEBUG FIELD AT THE application_id LEVEL
    COUNT(1) AS debug_num_rows,
    COUNT(DISTINCT application_id) AS debug_num_unique_applications,
    SUM(has_reached_approval::INT) AS debug_sum_has_reached_approval,
    COLLECT_LIST(
        STRUCT(
            application_id,
            ally_slug,
            ally_slugs_string,
            ally_clusters_string,
            ally_brands_string,
            ally_verticals_string,
            channel,
            synthetic_ocurred_on_local,
            minimum_ocurred_on_local,
            approval_ocurred_on_local,
            debug_application_datetime_local
        )
    ) AS debug_application_ids_array,
    /* COLLECT_LIST(
        STRUCT(
            application_id,
            application_process_baseline_id,
            client_type,
            ally_slug,
            ally_cluster,
            ally_brand,
            ally_vertical,
            ally_slugs_string,
            ally_clusters_string,
            ally_brands_string,
            ally_verticals_string,
            suborders_ally_slug_array,
            debug_application_datetime_local,
            synthetic_ocurred_on_local,
            minimum_ocurred_on_local,
            approval_ocurred_on_local,
            channel,
            original_product,
            synthetic_product_subcategory,
            synthetic_product_category
        )
    ) AS debug_all_fields, */
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM stg_funnel_step1_application_process_at_application_id_values_co_with_application_process_id
GROUP BY 1,2,3,4,5,6,7 -- IF LOGIC WORKS OK, IT'S GROUPING BY 1 ONLY
