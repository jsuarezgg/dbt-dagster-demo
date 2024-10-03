{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH loan_acceptance as (
    SELECT
    application_id,
    loan_acceptance_detail_json,
    from_utc_timestamp(ocurred_on, 'America/Bogota') as loan_acceptance_date_local
    FROM {{ source('silver_live', 'f_loan_acceptance_stage_co_logs') }}
    QUALIFY ROW_NUMBER() OVER (partition by application_id order by ocurred_on desc) = 1
),

ordered_experian_table AS (
  SELECT id_number,
  num_obl,
  reported_month,
  novedad,
  dpd,
  saldo_mora,
  name,
  report_date
  FROM {{ ref('f_bureau_reports_experian_co') }}
  ORDER BY num_obl,report_date desc
),

experian_report as (
    SELECT
    id_number,
    num_obl,
    COLLECT_LIST(NAMED_STRUCT(
    'reported_month',reported_month,
    'novedad',novedad,
    'report_date',report_date,
    'num_obl',num_obl,
    'days_past_due',dpd,
    'saldo_mora',saldo_mora,
    'name',name)) as bureau_report_detail_json
    FROM ordered_experian_table
    GROUP BY 1,2
),

order_installments as (
    SELECT
    loan_id,
    installment_principal,
    installment_pmt,
    installment_due_date
    FROM {{ source('silver_live', 'f_origination_installments_co') }}
    ORDER BY loan_id, installment_due_date asc
),

loan_installments as (
    SELECT
    loan_id,
        COLLECT_LIST(NAMED_STRUCT(
    'installment_principal',installment_principal,
    'installment_pmt',installment_pmt,
    'installment_due_date',installment_due_date)) as installments_detail_json
    FROM order_installments
    GROUP BY 1
)

SELECT
orig.client_id,
orig.application_id,
orig.loan_id,
orig.ally_slug,
orig.origination_date_local,
orig.approved_amount,
orig.term,
fl.state,
orig.interest_rate,
rmt.fully_paid_date,
exp.num_obl,
exp.bureau_report_detail_json,
las.loan_acceptance_detail_json,
loan_acceptance_date_local,
li.installments_detail_json
FROM addi_prod.gold.dm_originations orig
LEFT JOIN {{ source('silver_live', 'f_fincore_loans_co') }} fl ON fl.loan_id=orig.loan_id
LEFT JOIN loan_installments li ON li.loan_id=orig.loan_id
LEFT JOIN {{ source('gold', 'rmt_loan_fully_paid_date_co') }} rmt ON orig.loan_id=rmt.loan_id
LEFT JOIN {{ source('silver_live', 'd_prospect_personal_data_co') }} ppd ON ppd.client_id=orig.client_id
LEFT JOIN experian_report exp ON right(orig.loan_id,12)=right(exp.num_obl,12) and ppd.id_number=cast(exp.id_number as int)
LEFT JOIN loan_acceptance las ON las.application_id=orig.application_id 