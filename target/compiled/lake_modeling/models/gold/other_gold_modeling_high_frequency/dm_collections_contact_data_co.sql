

-- PERSONAL INFO
-- Identify the last application date
WITH query1 AS (
SELECT DISTINCT client_id,
max(application_date) AS last_app_date
FROM silver.f_pii_applications_co
WHERE application_cellphone IS NOT NULL
GROUP BY 1
),
-- Identify the cellphone and email associated to the last application date.
query2 AS (
SELECT DISTINCT (q1.client_id),
id_number,
application_cellphone,
application_email,
application_date
FROM silver.f_pii_applications_co pa
LEFT JOIN query1 q1
     ON pa.client_id = q1.client_id
WHERE application_date = q1.last_app_date
),
-- Get the firt name, last name and full name
query3 AS (
SELECT client_id,
full_name,
last_name,
TRIM(RIGHT(full_name , LENGTH(full_name) - CHARINDEX(' ', full_name))) AS second_name
FROM silver.d_prospect_personal_data_co
)

-- Put together all the personal info from different sources

SELECT DISTINCT (pa.client_id),
q2.id_number,
q2.application_cellphone,
q2.application_email,
q2.application_date AS last_application_date,
q3.full_name,
RIGHT(q3.second_name , LENGTH(q3.second_name) - CHARINDEX(' ', q3.second_name)) AS first_name,
q3.last_name
FROM silver.f_pii_applications_co pa
LEFT JOIN silver.f_originations_bnpl_co bnpl
  ON pa.application_id=bnpl.application_id
LEFT JOIN query3 q3
  ON pa.client_id = q3.client_id
LEFT JOIN query2 q2
  ON pa.client_id = q2.client_id
WHERE loan_id IS NOT NULL
ORDER BY last_application_date DESC