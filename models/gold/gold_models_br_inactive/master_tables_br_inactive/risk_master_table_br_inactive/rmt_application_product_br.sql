{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH legacy_application_product AS (
    select
      application_id,
      product as application_product,
       'PAGO' AS evaluation_type,
      case
        when journey_name ilike '%preap%' then 'PREAPPROVAL_BR'
        when application_date <= '2021-05-31' then 'PAGO_BR'
        when (journey_name ilike '%bnpn%' or a.custom_is_bnpn_branched = True) then 'BNPN_BR'
        else 'PAGO_BR'
      end as product
    from {{ ref('f_applications_br') }} a
)
,
aux_addi_intro_estimation AS (
    SELECT
        lp.application_id
        , 'PAGO' AS evaluation_type
        , credit_policy_name
        , product
        , count(*) AS loan_proposals
        , min(total_interest) AS min_total_interest
    FROM {{ ref('f_loan_proposals_br') }} lp
    LEFT JOIN {{ ref('f_underwriting_fraud_stage_br') }} uw     ON lp.application_id = uw.application_id
    LEFT JOIN {{ ref('f_applications_br') }} a                  ON lp.application_id = a.application_id
    WHERE 1=1
    AND a.product = 'PAGO_BR'
    AND uw.credit_policy_name LIKE '%FO'
    GROUP BY 1,2,3,4
    HAVING count(*) = 3
    AND min(total_interest) > 0
)
,
aux_transaction_type AS (
    SELECT
        a.application_id,
        bnpl.loan_id,
        CASE WHEN a.custom_is_bnpn_branched IS TRUE OR a.journey_name ILIKE '%BNPN%' THEN 'BNPN' ELSE 'BNPL' END AS custom_transaction_type
    FROM {{ ref('f_applications_br') }} a
    LEFT JOIN {{ ref('f_originations_bnpl_br') }} bnpl      ON a.application_id = bnpl.application_id
)
,
aux_addi_flex AS (
    SELECT
        orig.application_id,
        orig.loan_id,
        lp.total_interest,
        orig.custom_transaction_type
    FROM aux_transaction_type orig
    LEFT JOIN {{ ref('f_loan_proposals_br') }} lp       ON orig.loan_id = lp.loan_proposal_id
)
,
synthetic_product_category AS (
    SELECT
        lap.application_id,
        flex.loan_id,
        lap.application_product,
        lap.product,
        lap.evaluation_type,
        flex.custom_transaction_type,
        flex.total_interest,
        CASE
            WHEN intro.application_id IS NOT NULL THEN 'Intro'
            WHEN lap.product ilike '%FINANCIA%' THEN 'Extra'
            ELSE 'Cupo' END AS synthetic_product_category
    FROM legacy_application_product lap
    LEFT JOIN aux_addi_intro_estimation intro   ON lap.application_id = intro.application_id
    LEFT JOIN aux_addi_flex flex                ON lap.application_id = flex.application_id
)
,
synthetic_product_category_flex AS (
    SELECT
        application_id,
        loan_id,
        application_product,
        product,
        evaluation_type,
        custom_transaction_type,
        synthetic_product_category,
        CASE WHEN synthetic_product_category = 'Cupo' AND CAST(total_interest AS NUMERIC) > 0 THEN TRUE ELSE FALSE END AS is_flex
    FROM synthetic_product_category
)
SELECT
    application_id,
    loan_id,
    application_product,
    product,
    evaluation_type,
    is_flex,
    custom_transaction_type,
    synthetic_product_category,
    CASE
        WHEN custom_transaction_type = 'BNPL' AND is_flex IS TRUE THEN 'BNPL Flex'
        WHEN custom_transaction_type = 'BNPL' AND is_flex IS FALSE THEN 'BNPL 0%'
    ELSE custom_transaction_type END AS synthetic_product_subcategory
FROM synthetic_product_category_flex