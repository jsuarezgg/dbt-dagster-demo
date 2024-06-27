{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH clients_full_name AS (
  SELECT DISTINCT
		client_id,
		full_name
  FROM {{ ref('d_client_management_clients_co') }}
),

disburments_amounts AS (
  SELECT
		client_id,
		sum(coalesce(amount,0)) as amount
  FROM {{ ref('f_client_management_reimbursements_co') }}
  WHERE CAST(annulled AS boolean) = False
  GROUP BY 1
),

clc AS (
    SELECT
		lns.client_id,
		aps.id_number AS client_id_number,
		max(round(clt.positive_balance, 4)) AS positive_balance,
		bool_and(CASE WHEN cls.loan_id IS NOT NULL THEN True ELSE False END) AS is_cancelled,
		bool_and(lns.is_fully_paid) AS is_fully_paid,
		max(aps.application_cellphone) AS application_cellphone
	FROM addi_prod.silver.f_fincore_loans_co lns
	LEFT JOIN {{ ref('f_pii_applications_co') }} aps ON lns.client_id = aps.client_id
	LEFT JOIN {{ ref('f_fincore_clients_co') }} clt ON lns.client_id = clt.client_id
	LEFT JOIN {{ ref('f_loan_cancellations_v2_co') }} cls ON lns.loan_id = cls.loan_id
	GROUP BY 1, 2
),

final_table AS (

SELECT DISTINCT
    clc.client_id,
	clc.client_id_number,
	cfn.full_name,
	clc.application_cellphone,
	clc.positive_balance, 
	coalesce(r.amount,0) AS reimbursement_amount, 
	clc.positive_balance-coalesce(r.amount,0) as positive_balance_reimbursement_difference, 
	clc.is_fully_paid,
	clc.is_cancelled
FROM clc

LEFT JOIN disburments_amounts r
    ON r.client_id = clc.client_id

LEFT JOIN clients_full_name cfn
	ON clc.client_id = cfn.client_id

WHERE True
	AND (clc.positive_balance <> 0 or round(r.amount, 2) <> 0)
ORDER BY clc.client_id
)

select *
from final_table;