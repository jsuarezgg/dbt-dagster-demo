{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with ally_vertical as (
    select ally_slug, max(ally_vertical:['name']['value']) as vertical
    from {{ ref('d_ally_management_stores_allies_co') }}
    group by 1),
    ally_vertical_br as (
    select ally_slug, max(ally_vertical:['name']['value']) as vertical
    from {{ ref('d_ally_management_stores_allies_br') }}
    group by 1),
    applications_data as (
    select
      apps.client_id,
      count(*) as n_applications,
      count(
        case
          when application_date > `current_date`() - interval '1 week' then 1
          else null
        end
      ) as n_applications_last_week,
      count(
        case
          when application_date > `current_date`() - interval '1 month' then 1
          else null
        end
      ) as n_applications_last_month,
      count(
        case
          when orig.application_id is not null then 1
          else null
        end
      ) as n_originations,
      count(*) FILTER (
        WHERE
          orig.application_id is not null
          and product = 'PAGO_CO'
      ) as n_pago_originated_loans,
      count(*) FILTER (
        WHERE
          orig.application_id is not null
          and application_date > `current_date`() - interval '1 week'
      ) as n_originations_last_week,
      count(*) FILTER (
        WHERE
          orig.application_id is not null
          and application_date > `current_date`() - interval '1 month'
      ) as n_originations_last_month,
      max(application_date) as last_application_date,
      sum(
        case
          when orig.application_id is not null then approved_amount
          else 0
        end
      ) as total_GMV_originated,
      avg(approved_amount) as avg_ammount_loan,
      max(approved_amount) as max_ammount_loan,
      count(*) as n_slugs,
      count(distinct apps.ally_slug) as n_different_slugs,
      count(distinct av.vertical) as n_different_verticals,
      count(*) FILTER(
        WHERE
          channel in ('E_COMMERCE_CUSTOM', 'E_COMMERCE_VTEX', 'E_COMMERCE_SHOPIFY')
          and orig.application_id is not null
      ) as n_eccom_originated_loans,
      count(*) FILTER(
        WHERE
          channel not in ('E_COMMERCE_CUSTOM', 'E_COMMERCE_VTEX', 'E_COMMERCE_SHOPIFY')
          and orig.application_id is not null
      ) as n_physical_originated_loans
    from
      {{ source('silver_live', 'f_applications_co') }} apps
    left join
      {{ source('silver_live', 'f_originations_bnpl_co') }} orig
    on
      apps.application_id = orig.application_id
    left join
      ally_vertical av
    on
      apps.ally_slug = av.ally_slug
    group by
      1
  ),
  applications_ordered as (
    select
      apps.client_id,
      application_date,
      case when orig.application_id is not null then true else false end as loan_accepted,
      orig.approved_amount,
      apps.ally_slug,
      av.vertical,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date desc
      ) as application_last,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date asc
      ) as application_first
    from
      {{ source('silver_live', 'f_applications_co') }} apps
    left join
      {{ source('silver_live', 'f_originations_bnpl_co') }} orig
    on
      apps.application_id = orig.application_id
    left join
      ally_vertical av
    on
      apps.ally_slug = av.ally_slug
    order by
      client_id,
      application_date desc
  ),
  originations_ordered as (
    select
      apps.client_id,
      apps.application_date,
      true as loan_accepted,
      orig.approved_amount,
      apps.ally_slug,
      av.vertical,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date desc
      ) as origination_last,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date asc
      ) as origination_first
    from
      {{ source('silver_live', 'f_applications_co') }} apps
    inner join
      {{ source('silver_live', 'f_originations_bnpl_co') }} orig
    on
      apps.application_id = orig.application_id
    left join
      ally_vertical av
    on
      apps.ally_slug = av.ally_slug
    order by
      client_id,
      application_date desc
  ),
  last_application as (
    select
      *
    from
      applications_ordered
    where
      application_last = 1
  ),
  first_application as (
    select
      *,
      datediff(`current_date`(), application_date) as n_active_days
    from
      applications_ordered
    where
      application_first = 1
  ),
  last_origination as (
    select *
    from
      originations_ordered
    where
      origination_last = 1
  ),
  first_origination as (
    select *
    from
      originations_ordered
    where
      origination_first = 1
  ),
  applications_timed as (
    select
      la.client_id,
      la.ally_slug as last_slug,
      la.vertical as last_vertical,
      fa.ally_slug as first_slug,
      fa.vertical as first_vertical,
      fa.n_active_days,
      fa.application_date as first_origination_date,
      lo.application_date as last_origination_date,
      lo.approved_amount as last_ammount_loan
    from
      last_application la full
      outer join first_application fa on la.client_id = fa.client_id full
      outer join last_origination lo on la.client_id = lo.client_id
  ),
  loan_status as (
    select
      client_id,
      count(distinct loan_id) as n_active_loans
    from
      {{ ref('d_syc_loan_status_co') }}
    group by
      1
  ),
  syc_clients as (
    select
      client_id,
      remaining_addicupo,
      addicupo_state,
      addicupo_last_update
    from
      {{ ref('d_syc_clients_co') }}
  ),
  incomes as (
    select
      client_id,
      collect_list(income_estimatedIncome) as estimated_income
    from
      {{ source('silver_live', 'f_kyc_bureau_income_estimator_co') }}
    group by
      1
  ),
  client_loan_status_features as (
    select
      client_id
      ,min(calculation_date)filter(where paid_installments > 0) as first_installment_paid_date
      ,min(calculation_date)filter(where is_fully_paid is true) as first_fully_paid_loan_date
      ,max(days_past_due) as max_days_past_due
    from {{ source('gold', 'dm_daily_loan_status_co') }}
    group by 1
  ),
  installments as (
    select client_id
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
    from {{ ref('f_snc_payments_report_co') }}
    group by 1
  ),
  contacts as (
    SELECT
        customer_id AS customerId,
        client_id,
        min(sent_at) AS first_contact
    FROM {{ ref('f_kustomer_messages') }} km
    INNER JOIN {{ ref('d_kustomer_crm_aggregator_clients_co') }} cc 	ON km.customer_id = cc.kustomer_id
    GROUP BY 1, 2
  ),
  pii_information as (
    SELECT client_id
          ,max(personId_ageRange) as prospect_age_range
          ,case when max(personId_ageRange) = "18-21" then 19.5
                when max(personId_ageRange) = "18-25" then 21.5
                when max(personId_ageRange) = "22-28" then 25.0
                when max(personId_ageRange) = "25-30" then 27.5
                when max(personId_ageRange) = "29-35" then 32.0
                when max(personId_ageRange) = "31-35" then 33.0
                when max(personId_ageRange) = "36-40" then 38.0
                when max(personId_ageRange) = "36-45" then 40.5
                when max(personId_ageRange) = "41-45" then 43.0
                when max(personId_ageRange) = "46-50" then 48.0
                when max(personId_ageRange) = "46-55" then 50.5
                when max(personId_ageRange) = "51-55" then 53.0
                when max(personId_ageRange) = "56-60" then 58.0
                when max(personId_ageRange) = "56-65" then 60.5
                when max(personId_ageRange) = "61-65" then 63.0
                when max(personId_ageRange) = "66-70" then 68.0
                when max(personId_ageRange) = "71-75" then 73.0
                when max(personId_ageRange) = "Mas 75" then 78.0 
           end as prospect_age_avg
           --,row_number() over(PARTITION by client_id order by ocurred_on_date DESC) AS rn
        FROM {{ source('silver_live', 'f_kyc_bureau_personal_info_co') }}
        GROUP BY client_id
        --QUALIFY rn = 1
  ),
  final_table_co as (
    select
      'CO' as country,
      ad.*,
      i.estimated_income,
      ati.last_ammount_loan,
      ati.last_slug,
      ati.last_vertical,
      ati.first_slug,
      ati.first_vertical,
      ati.first_origination_date,
      ati.last_origination_date,
      ati.n_active_days,
      ls.n_active_loans,
      sc.remaining_addicupo,
      sc.addicupo_state,
      sc.addicupo_last_update,
      cpd.first_installment_paid_date,
      cpd.first_fully_paid_loan_date,
      cpd.max_days_past_due,
      ins.installment_paid_in_delinquency_n,
      ins.installment_paid_in_delinquency_proportion,
      con.first_contact,
      pii.prospect_age_range,
      pii.prospect_age_avg
    from
      applications_data ad
      left join applications_timed ati on ad.client_id = ati.client_id
      left join loan_status ls on ad.client_id = ls.client_id
      left join syc_clients sc on ad.client_id = sc.client_id
      left join incomes i on ad.client_id = i.client_id
      left join client_loan_status_features cpd on ad.client_id = cpd.client_id
      left join installments ins on ad.client_id = ins.client_id
      left join contacts con on ad.client_id = con.client_id
      left join pii_information pii ON ad.client_id = pii.client_id
  ),
  applications_data_br as (
    select
      apps.client_id,
      count(*) as n_applications,
      count(
        case
          when application_date > `current_date`() - interval '1 week' then 1
          else null
        end
      ) as n_applications_last_week,
      count(
        case
          when application_date > `current_date`() - interval '1 month' then 1
          else null
        end
      ) as n_applications_last_month,
      count(
        case
          when (orig.application_id is not null or orig_bnpn.application_id is not null) then 1
          else null
        end
      ) as n_originations,
      count(*) FILTER (
        WHERE
          (orig.application_id is not null or orig_bnpn.application_id is not null)
          and product = 'PAGO_CO'
      ) as n_pago_originated_loans,
      count(*) FILTER (
        WHERE
          (orig.application_id is not null or orig_bnpn.application_id is not null)
          and application_date > `current_date`() - interval '1 week'
      ) as n_originations_last_week,
      count(*) FILTER (
        WHERE
          (orig.application_id is not null or orig_bnpn.application_id is not null)
          and application_date > `current_date`() - interval '1 month'
      ) as n_originations_last_month,
      max(application_date) as last_application_date,
      sum(
        case
          when orig.application_id is not null or orig_bnpn.application_id is not null then approved_amount
          else 0
        end
      ) as total_GMV_originated,
      avg(approved_amount) as avg_ammount_loan,
      max(approved_amount) as max_ammount_loan,
      count(*) as n_slugs,
      count(distinct apps.ally_slug) as n_different_slugs,
      count(distinct vertical) as n_different_verticals,
      count(*) FILTER(
        WHERE
          channel in ('E_COMMERCE_CUSTOM', 'E_COMMERCE_VTEX')
          and (orig.application_id is not null or orig_bnpn.application_id is not null)
      ) as n_eccom_originated_loans,
      count(*) FILTER(
        WHERE
          channel not in ('E_COMMERCE_CUSTOM', 'E_COMMERCE_VTEX')
          and (orig.application_id is not null or orig_bnpn.application_id is not null)
      ) as n_physical_originated_loans
    from
      {{ ref('f_applications_br') }} apps
    left join
      {{ ref('f_originations_bnpl_br') }} orig
    on
      apps.application_id = orig.application_id
    left join
      {{ ref('f_originations_bnpn_br') }} orig_bnpn
    on
      apps.application_id = orig_bnpn.application_id
    left join
      ally_vertical_br av
    on
      apps.ally_slug = av.ally_slug
    group by
      1
  ),
  applications_ordered_br as (
select
      apps.client_id,
      application_date,
      case when orig.application_id is not null or orig_bnpn.application_id is not null then true else false end as loan_accepted,
      coalesce(orig.approved_amount, orig_bnpn.requested_amount) as approved_amount,
      apps.ally_slug,
      av.vertical,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date desc
      ) as application_last,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date asc
      ) as application_first
    from
      {{ ref('f_applications_br') }} apps
    left join
      {{ ref('f_originations_bnpl_br') }} orig
    on
      apps.application_id = orig.application_id
    left join
      {{ ref('f_originations_bnpn_br') }} orig_bnpn
    on
      apps.application_id = orig_bnpn.application_id
    left join
      ally_vertical_br av
    on
      apps.ally_slug = av.ally_slug
    order by
      client_id,
      application_date desc
  ),
 originations_ordered_br as (
    select
      apps.client_id,
      application_date,
      case when orig.application_id is not null or orig_bnpn.application_id is not null then true else false end as loan_accepted,
      coalesce(orig.approved_amount, orig_bnpn.requested_amount) as approved_amount,
      apps.ally_slug,
      av.vertical,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date desc
      ) as origination_last,
      row_number() over (
        PARTITION BY apps.client_id
        ORDER BY
          application_date asc
      ) as origination_first
    from
      {{ ref('f_applications_br') }} apps
    left join
      {{ ref('f_originations_bnpl_br') }} orig
    on
      apps.application_id = orig.application_id
    left join
      {{ ref('f_originations_bnpn_br') }} orig_bnpn
    on
      apps.application_id = orig_bnpn.application_id
    left join
      ally_vertical_br av
    on
      apps.ally_slug = av.ally_slug
    where orig.application_id is not null or orig_bnpn.application_id is not null
    order by
      client_id,
      application_date desc
  ),
  last_application_br as (
    select
      *
    from
      applications_ordered_br
    where
      application_last = 1
  ),
  first_application_br as (
    select
      *,
      datediff(`current_date`(), application_date) as n_active_days
    from
      applications_ordered_br
    where
      application_first = 1
  ),
  last_origination_br as (
    select *
    from
      originations_ordered_br
    where
      origination_last = 1
  ),
  first_origination_br as (
    select *
    from
      originations_ordered_br
    where
      origination_first = 1
  ),
  applications_timed_br as (
    select
      la.client_id,
      la.ally_slug as last_slug,
      la.vertical as last_vertical,
      fa.ally_slug as first_slug,
      fa.vertical as first_vertical,
      fa.n_active_days,
      fa.application_date as first_origination_date,
      lo.application_date as last_origination_date,
      lo.approved_amount as last_ammount_loan
    from
      last_application_br la full
      outer join first_application_br fa on la.client_id = fa.client_id full
      outer join last_origination_br lo on la.client_id = lo.client_id
  ),
  loan_status_br as (
    select
      client_id,
      count(distinct loan_id) as n_active_loans
    from
      {{ ref('d_syc_loan_status_br') }}
    group by
      1
  ),
  syc_clients_br as (
    select
      client_id,
      remaining_addicupo,
      addicupo_state,
      addicupo_last_update
    from
      {{ ref('d_syc_clients_br') }}
  ),
  incomes_br as (
    select
      client_id,
      collect_list(income_estimatedIncome) as estimated_income
    from
      {{ ref('f_kyc_bureau_income_estimator_br') }}
    group by
      1
  ),
  client_loan_status_features_br as (
    select
      client_id
      ,min(calculation_date)filter(where paid_installments > 0) as first_installment_paid_date
      ,min(calculation_date)filter(where is_fully_paid is true) as first_fully_paid_loan_date
      ,max(days_past_due) as max_days_past_due
    from {{ source('gold', 'dm_daily_loan_status_br') }}
    group by 1
  ),
  installments_br as (
    select client_id
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
    from {{ ref('f_snc_payments_report_br') }}
    group by 1
  ),
  contacts_br as (
	SELECT
      km.customer_id AS customerId,
      cc.client_id,
      min(km.sent_at) AS first_contact
    FROM {{ ref('f_kustomer_messages') }} km
    LEFT JOIN {{ ref('kustomer_customers') }} kc            ON km.customer_id = kc.customer_id
    INNER JOIN {{ ref('d_prospect_personal_data_br') }} cc  ON kc.external_id = cc.client_id
    group by 1, 2
  ),
  final_table_br as (
    select
      'BR' as country,
      ad.*,
      i.estimated_income,
      ati.last_ammount_loan,
      ati.last_slug,
      ati.last_vertical,
      ati.first_slug,
      ati.first_vertical,
      ati.first_origination_date,
      ati.last_origination_date,
      ati.n_active_days,
      ls.n_active_loans,
      sc.remaining_addicupo,
      sc.addicupo_state,
      sc.addicupo_last_update,
      cpd.first_installment_paid_date,
      cpd.first_fully_paid_loan_date,
      cpd.max_days_past_due,
      ins.installment_paid_in_delinquency_n,
      ins.installment_paid_in_delinquency_proportion,
      con.first_contact, 
      NULL AS prospect_age_range,
      NULL AS prospect_age_avg
    from
      applications_data_br ad
      left join applications_timed_br ati on ad.client_id = ati.client_id
      left join loan_status_br ls on ad.client_id = ls.client_id
      left join syc_clients_br sc on ad.client_id = sc.client_id
      left join incomes_br i on ad.client_id = i.client_id
      left join client_loan_status_features_br cpd on ad.client_id = cpd.client_id
      left join installments_br ins on ad.client_id = ins.client_id
      left join contacts_br con on ad.client_id = con.client_id
  )
  select
    *
  from
    final_table_co
  union all
  select
    *
  from
    final_table_br
