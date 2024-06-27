{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- LOANTAPE INFO

--Pull info from loan status
WITH query1 AS (
SELECT
DISTINCT (ls.client_id)
,COUNT(ls.loan_id) AS n_loans
,COUNT(rmt.loan_id) FILTER (where rmt.product='PAGO_CO') number_pago_loans
,COUNT(rmt.loan_id) FILTER (where rmt.product='FINANCIA_CO') number_financia_loans
,COUNT(ls.loan_id) FILTER (WHERE ls.days_past_due > 0) AS dq_loans
,SUM(ls.principal_overdue) AS client_principal_overdue
,SUM(ls.interest_overdue) AS client_interest_overdue
,SUM(ls.interest_on_overdue_principal) AS client_interest_on_overdue_principal
,SUM(ls.guarantee_overdue) AS client_guarantee_overdue
,SUM(ls.principal_overdue) + SUM(ls.interest_overdue) + SUM(ls.interest_on_overdue_principal) + SUM(ls.guarantee_overdue) AS client_total_overdue
,SUM(ls.full_payment) AS client_full_payment
,SUM(ls.unpaid_principal) AS client_upb
,SUM(ls.approved_amount) AS client_opb
,MIN(ls.origination_date)::date AS client_first_origination
,MAX(ls.origination_date)::date AS client_last_origination
,MIN(ls.days_past_due) AS min_dpd
,MAX(ls.days_past_due) AS max_dpd
,CASE
  WHEN MAX(ls.days_past_due) = 0 THEN 0
  WHEN MAX(ls.days_past_due) BETWEEN 1 AND 30 THEN 1
  WHEN MAX(ls.days_past_due) BETWEEN 31 AND 60 THEN 2
  WHEN MAX(ls.days_past_due) BETWEEN 61 AND 90 THEN 3
  WHEN MAX(ls.days_past_due) BETWEEN 91 AND 120 THEN 4
  WHEN MAX(ls.days_past_due) > 120 THEN 5
  ELSE 0
  END
  AS max_bucket_num
,CASE
  WHEN MAX(ls.days_past_due) = 0 THEN 'Current'
  WHEN MAX(ls.days_past_due) BETWEEN 1 AND 30 THEN '1 to 30'
  WHEN MAX(ls.days_past_due) BETWEEN 31 AND 60 THEN '31 to 60'
  WHEN MAX(ls.days_past_due) BETWEEN 61 AND 90 THEN '61 to 90'
  WHEN MAX(ls.days_past_due) BETWEEN 91 AND 120 THEN '91 to 120'
  WHEN MAX(ls.days_past_due) > 120 THEN '120+'
  ELSE 'Current'
  END
  AS max_bucket_name
,SUM(ls.min_payment) AS min_payment
,MIN(ls.first_payment_date)::date AS first_first_payment_date
FROM {{ ref('dm_loan_status_co') }} ls
LEFT JOIN {{ ref('risk_master_table_co') }} rmt ON rmt.loan_id=ls.loan_id
WHERE ls.cancellation_reason IS NULL
AND ls.is_fully_paid IS FALSE
GROUP BY 1
)

,query2 AS
(SELECT
client_id,
MAX(fp.payment_date)::date AS last_payment_date,
(datediff(current_date, MAX(fp.payment_date::date))) <= 3 AS 3_days_payment
FROM addi_prod.silver.f_fincore_payments_co fp
GROUP BY 1
)

,query3 AS (
SELECT 
sri.client_id
,COUNT(sri.id) AS n_refinancing
,MAX(sri.start_date)::date AS refinancing_last_start
,MAX(
    CASE
        WHEN sri.end_date IS NOT NULL THEN sri.end_date
        ELSE date_add(sri.start_date, 90)
        END
    )::date AS refinancing_last_end
FROM {{ ref('f_syc_refinancing_instructions_co') }} sri
WHERE annulled IS FALSE
GROUP BY 1
)
--AGREGAR ACÁ QUERIES DE LAS TABLAS 6 A 8

,query4 AS (
WITH attempts AS (
SELECT
client_id
,MAX(start_at) AS last_attempt
,SUM(
    CASE
        WHEN datediff(current_date, start_at) < 31 THEN 1
        ELSE 0
        END
) AS last_month_attempts
FROM {{ ref('f_talkdesk_calls_report_co') }}
WHERE (team_name in ('Team_Collections_Colombia','Team_Digital_Collections_Colombia','Team_Teleperformance_Collections') or team_id is null)
GROUP BY 1
)
,contacts AS (
SELECT
client_id
,MAX(start_at) AS last_direct_contact
,SUM(CASE WHEN datediff(current_date, start_at) < 31 THEN 1 ELSE 0 END) AS last_month_direct_contacts
FROM {{ ref('f_talkdesk_calls_report_co') }}
WHERE team_name in ('Team_Collections_Colombia','Team_Digital_Collections_Colombia','Team_Teleperformance_Collections')
AND disposition_code in ('Contactado','llamada_no_completada') and start_at is not null
GROUP BY 1)

SELECT
att.client_id
,att.last_attempt
,att.last_month_attempts
,con.last_direct_contact
,con.last_month_direct_contacts
FROM attempts att
LEFT JOIN contacts con
    ON att.client_id = con.client_id
)
,query5 as (
    SELECT
        resourceid,
        LastResolTreeValue
    FROM (
        SELECT
            conversation_id AS resourceid,
            reverse(split(custom_attribute_value,'[.]'))[0] AS LastResolTreeValue,
            reference_date AS referencedate,
            max(reference_date) OVER (PARTITION BY conversation_id) AS max_date
        FROM {{ ref('f_kustomer_conversations_custom_attributes') }}
        WHERE custom_attribute_name LIKE '%resolutionCollectionsTree%'
    )
    WHERE referencedate = max_date
),

query6 as (
    SELECT *
    FROM (
      SELECT
          crm.client_id,
          CASE
              WHEN array_contains(con.tags,'5fa58a9f00df9f1568d36795') OR
              array_contains(con.tags,'5fa58ae076e0b4e135b045a1') OR
              array_contains(con.tags,'611530d834f003463851fda2') OR
              array_contains(con.tags,'60aee3f40a9008aeb8af6c8f') OR
              array_contains(con.tags,'6035a45bc6618184385c7c90') OR
              array_contains(con.tags,'61e02db76d9230a10ca781a3') OR
              array_contains(con.tags,'61e977ec2e38b93151ed794d') OR
              array_contains(con.tags,'61ead517938d91443f0b83f3') OR
              array_contains(con.tags,'615637e9e71ddd41ba3fe225') OR
              array_contains(con.tags,'6160ad90d84a418bef020c4a') OR
              array_contains(con.tags,'6160ad56969526d04c2cb499') OR
              array_contains(con.tags,'6160acfa68ead80cf0061205') OR
              array_contains(con.tags,'61c9e7b13e5ab3d4cd98e55c') OR
              array_contains(con.tags,'61a7f55a92b26d643b6590fd') OR
              array_contains(con.tags,'6099a3001f5724932ca4f8e5') OR
              array_contains(con.tags,'60183305aa81db2d55e122e0') OR
              array_contains(con.tags,'606781a03cd46e136227725b') THEN 'Cancelacion_Garantia'
          ELSE LastResolTreeValue
          END as ResolTreeType,
          datediff(current_date, created_at) AS DaysDiffLastResol
      FROM {{ ref('f_kustomer_conversations') }} con
      INNER JOIN {{ ref('d_kustomer_crm_aggregator_clients_co') }} crm  ON con.customer_id = crm.kustomer_id
      LEFT JOIN query5 q5                                               ON con.conversation_id = q5.resourceid
    )
    PIVOT(min(DaysDiffLastResol) FOR ResolTreeType in ('Cancelacion_Garantia','error_de_marcador','abandono1','posible_cancelacion','posible_fraude','congelamiento_cuotas',
    'condonacion','equivocado','buzon_de_mensajes','ya_pago','llamada_no_completada','addi_plan','no_contesta','recordar_pago','callback',
    'sin_compromiso','fallecido','preventivo','titular_cuelga','promesa_de_pago','llamada_de_bienvenida','hablo_con_tercero','cuelga',
    'reduccion_cuotas'))
),

query7 AS (
SELECT case when cpp.state='CREATED' then 'pending'
else lower(cpp.state) end as state,
cpp.client_id,
cpp.expected_amount,
cpp.start_date,
CASE
WHEN isnull(fp.paid_amount) THEN 0
ELSE fp.paid_amount
END as paid_amount,
from_utc_timestamp(end_date,'America/Bogota') end_date,
max(cpp.ocurred_on_date) OVER (partition by cpp.client_id,cpp.start_date) as max_ocurred_on,
max(cpp.start_date) OVER (partition by cpp.client_id) as max_start_date,
CASE
WHEN from_utc_timestamp(cpp.end_date,'America/Bogota')>=current_date() THEN TRUE
ELSE FALSE
END as active_promise,
cpp.ocurred_on_date ocurred_on
FROM {{ ref('f_client_payment_promises_co') }} cpp
LEFT JOIN addi_prod.silver.f_fincore_payments_co fp
ON cpp.client_payment_id=fp.payment_id
WHERE NOT(from_utc_timestamp(end_date,'America/Bogota') < current_date() AND state='PENDING')),

-- query8 AS (
-- SELECT
-- client_id
-- ,max(from_date) AS last_from_date
-- FROM {{ source('cur','payment_promises') }}
-- GROUP BY 1),

-- query9 AS (
-- SELECT DISTINCT
-- pp.promise_status AS state,
-- pp.client_id,
-- pp.payment_promise_amount AS expected_amount,
-- CASE
-- WHEN isnull(fp.paid_amount) THEN 0
-- ELSE fp.paid_amount
-- END as paid_amount,
-- pp.from_date AS start_date,
-- pp.due_date AS end_date,
-- CASE
-- WHEN pp.due_date>=current_date() THEN TRUE
-- ELSE FALSE
-- END
-- AS active_promise
-- FROM {{ source('cur','payment_promises') }} pp
-- LEFT JOIN addi_prod.silver.f_fincore_payments_co fp
-- ON element_at(pp.payments_ids,1)=fp.payment_id
-- LEFT JOIN query8 q8
--     ON pp.client_id = q8.client_id
-- WHERE pp.from_date = q8.last_from_date
-- AND NOT(pp.due_date < current_date() AND pp.promise_status='pending')
-- UNION
-- (SELECT DISTINCT state,
-- client_id,
-- expected_amount,
-- paid_amount,
-- start_date,
-- end_date,
-- active_promise
-- FROM query7
-- WHERE ocurred_on=max_ocurred_on 
-- and start_date=max_start_date
-- )),

-- query10 AS (SELECT client_id,
-- lower(state) as state,
-- expected_amount,
-- paid_amount,
-- start_date,
-- end_date,
-- max(end_date) OVER (PARTITION BY client_id) as max_end_date,
-- active_promise
-- FROM query9
-- ),

query11 AS(
SELECT distinct client_id,
max(state) as state,
max(expected_amount) as expected_amount,
max(active_promise) as active_promise,
max(end_date) as end_date,
max(start_date::date) as start_date,
max(paid_amount) as paid_amount
FROM query7
GROUP BY client_id 
),

query12 AS (
SELECT fmt.*
,fl.state
,mt.application_number
,mt.loan_number
,mt.addi_pd
,mt.prospect_id client_id
,mt.product
,mt.idv_approved
,mt.idv_prev_approved
,CASE
    WHEN mt.idv_approved = 0 AND mt.idv_prev_approved = 0 THEN 1
    ELSE 0
    END
    AS idv_bypassed_or
FROM {{ ref('fraud_master_table_co') }} fmt
LEFT JOIN {{ ref('risk_master_table_co') }} mt
    ON fmt.application_id = mt.application_id
LEFT JOIN addi_prod.silver.f_fincore_loans_co fl
		ON fmt.loan_id=fl.loan_id
),

query13 AS (
SELECT
client_id
,loan_id
,CASE
    WHEN state = 'CANCELLED_BY_FRAUD' THEN 1
    WHEN (idv_bypassed_or = 1 AND (diff_vertical_monthly >= 1 or repeated_address = 1 or preapp_hesitation = 1 or diff_app_cellphone = 1 or full_preapp_amount = 1 or
    diff_ally_less_amount_monthly >= 1 or diff_ally_n_amount_monthly >= 1 or (addi_pd > 0.15 and phone_not_match = 1))) THEN 1
    WHEN (idv_bypassed_or = 0 AND (diff_app_cellphone = 1  or diff_vertical_monthly >= 1 or preapp_hesitation = 1 or weekly_apps >= 1 or diff_app_email = 1 or
    diff_apps_n_amount_monthly >= 1 or (case when loan_number = 1 then application_number else 0 end) > 5 or preapp_to_orig_minutes > 19497 or (addi_pd > 0.15 and phone_not_match = 1))) THEN 1
    ELSE 0
    END
    AS fraud_flag
FROM query12
),

query14 AS (
SELECT client_id
,origination_date
,ally_name
,max(origination_date) OVER (PARTITION BY client_id) as max_origination_date
FROM {{ ref('dm_loan_status_co') }}
),

query15 AS (
SELECT client_id
,ally_name
FROM query14
WHERE origination_date=max_origination_date
),

query16 AS (
SELECT q15.client_id
,max(ally_name) AS Last_Ally_Name
,concat(cast(cast(sum(fraud_flag)/count(fraud_flag)*100 as int) as string),'%') Fraud_Percentage
,CASE WHEN sum(fraud_flag)/count(fraud_flag)*100>49 THEN 1
ELSE 0
END as Fraud_Flag
FROM query15 q15
LEFT JOIN query13 q13
ON q13.client_id=q15.client_id
GROUP BY q15.client_id),

client_exclusion_list as (
SELECT client_id,
       reason
FROM {{ ref('f_client_exclusions_co_logs') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY ocurred_on DESC) = 1
)




SELECT
 q1.client_id
,q1.n_loans
,q1.number_financia_loans
,q1.number_pago_loans
,q1.dq_loans
,q1.client_principal_overdue
,q1.client_interest_overdue
,q1.client_interest_on_overdue_principal
,q1.client_guarantee_overdue
,q1.client_total_overdue
,q1.client_full_payment
,q1.client_upb
,q1.client_opb
,q1.client_first_origination
,q1.client_last_origination
,q1.min_dpd AS current_min_dpd
,q1.max_dpd AS current_max_dpd
,q1.max_bucket_num AS current_max_bucket_num
,q1.min_payment AS current_min_payment
,q1.first_first_payment_date
,q2.last_payment_date
,(datediff(current_date, q2.last_payment_date)) AS days_since_last_payment
,n_refinancing
,refinancing_last_start
,refinancing_last_end
,current_date <= refinancing_last_end AS active_refinancing
,CASE
  WHEN cel.reason IS NOT NULL THEN TRUE
  ELSE FALSE
  END
  AS excluded
,cel.reason AS exclusion_reason
,q4.last_attempt AS last_call_attempt
,q4.last_month_attempts AS last_month_call_attempts
,q4.last_direct_contact
,q4.last_month_direct_contacts
,CASE WHEN datediff(current_date , q4.last_direct_contact) < 31 THEN 'CD'
      WHEN datediff(current_date , q4.last_direct_contact) >= 31 THEN 'NC'
      WHEN isnull(q4.last_direct_contact) AND isnotnull(q4.last_attempt) THEN 'SK'
      ELSE NULL
      END AS direct_contact_sufix
,q6.Cancelacion_Garantia
,q6.error_de_marcador
,q6.abandono1
,q6.titular_cuelga
,q6.cuelga
,q6.llamada_no_completada
,q6.no_contesta
,q6.buzon_de_mensajes
,q6.equivocado
,q6.posible_cancelacion
,q6.posible_fraude
,q6.ya_pago
,q6.recordar_pago
,q6.callback
,q6.sin_compromiso
,q6.fallecido
,q6.preventivo
,q6.promesa_de_pago
,q6.llamada_de_bienvenida
,q6.hablo_con_tercero
,q6.congelamiento_cuotas
,q6.addi_plan
,q6.reduccion_cuotas
,q6.condonacion
,q11.state AS promise_status
,q11.expected_amount AS promise_expected_amount
,q11.paid_amount promise_paid_amount
,q11.start_date AS promises_last_start_date
,q11.end_date AS promises_last_end_date
,q11.active_promise
,q16.last_ally_name
,q16.fraud_percentage
,q16.fraud_flag

FROM query1 q1
LEFT JOIN query2 q2
  ON q1.client_id = q2.client_id
LEFT JOIN query3 q3
  ON q1.client_id = q3.client_id
LEFT JOIN client_exclusion_list cel
  ON q1.client_id = cel.client_id
LEFT JOIN query4 q4
  ON q1.client_id = q4.client_id
LEFT JOIN query6 q6
  ON q1.client_id = q6.client_id
LEFT JOIN query11 q11
  ON q1.client_id = q11.client_id
LEFT JOIN query16 q16
  ON q1.client_id = q16.client_id