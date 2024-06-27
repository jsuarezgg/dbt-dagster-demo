

WITH past_x_applications AS (
    SELECT 
        a.client_id,
        a.application_id,
        a.requested_amount,
        a.application_date,
        unix_timestamp(a.application_date) application_date_unix,
        ROW_NUMBER() OVER (PARTITION BY a.client_id ORDER BY a.application_date DESC) AS orig_order
     FROM silver.f_applications_co a
     LEFT JOIN gold.rmt_application_product_co ap     
         ON a.application_id = ap.application_id
     WHERE a.application_date >= add_months(current_date(),-18)
         AND lower(ap.product) LIKE '%pago_co%'
         AND lower(a.journey_name) NOT LIKE '%preapp%'
),
times AS (
    SELECT 
        *,
        (application_date_unix - LEAD(application_date_unix) OVER(PARTITION BY client_id ORDER BY orig_order)) / 60 AS dif_2_application_min,
        (application_date_unix - LEAD(application_date_unix,2) OVER(PARTITION BY client_id ORDER BY orig_order)) / 60 AS dif_3_application_min,
        LEAD(requested_amount) OVER (PARTITION BY client_id ORDER BY orig_order ASC) AS past_2_req_amt,
        LEAD(requested_amount,2) OVER (PARTITION BY client_id ORDER BY orig_order ASC) AS past_3_req_amt
    FROM past_x_applications
),
tagged AS (
    SELECT 
        client_id,
        application_id,
        application_date,
        orig_order,
        CASE
              WHEN dif_2_application_min <= 720 AND requested_amount < past_2_req_amt AND dif_3_application_min <= 720 AND past_2_req_amt < past_3_req_amt THEN 1 ELSE 0
        END AS tag_walkdown
     FROM times
)
SELECT 
    client_id,
    application_id,
    MAX(tag_walkdown) mr_walkdown
FROM tagged
WHERE application_id IS NOT NULL
GROUP BY 1,2