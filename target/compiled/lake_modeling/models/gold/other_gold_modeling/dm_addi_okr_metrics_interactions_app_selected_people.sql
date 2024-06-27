

WITH legacy_app_data_engagement AS (
    (select 
        'app_home_specific_store_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_home_specific_store_touched
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    --LIMIT 100
    )
    UNION ALL
    (
    select 
        'app_store_specific_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_store_specific_touched
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    --LIMIT 100
    )
    UNION ALL 
    (
    select 
        'app_store_tapped' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_store_tapped
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL 
    (
    select 
        'app_home_paynow_button_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_home_paynow_button_touched
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL 
    (
    select 
        'app_pix_form_paynow_button_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_pix_form_paynow_button_touched
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL 
    (
    select 
        'app_payments_paynow_button_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_payments_paynow_button_touched
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL 
    (
    select 
        'app_payment_methods_paynow_button_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_payment_methods_paynow_button_touched
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL 
    (
    select 
        'app_pay_button_tapped' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         client_id,
         country,
         _dt AS client_event_date,
         client_event_time
    from tru.app_pay_button_tapped
    where client_id is not NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
)
,
-- new_app_data_engagement_bronze AS (
--     SELECT *
--     FROM
--     ((
--         SELECT
--             'amplitude_addi_funnel_shop_store_tapped' AS my_source,
--             'new_bronze' AS my_project,
--             event_type AS original_event_name,
--             session_id,
--             user_id AS client_id,
--             country,
--             to_date(event_time) AS client_event_date,
--             to_timestamp(event_time) AS client_event_time
--         FROM bronze.amplitude_addi_funnel_shop_store_tapped
--     )
--     UNION ALL
--     (
--         SELECT
--             'amplitude_addi_funnel_home_store_tapped' AS my_source,
--             'new_bronze' AS my_project,
--             event_type AS original_event_name,
--             session_id,
--             user_id AS client_id,
--             country,
--             to_date(event_time) AS client_event_date,
--             to_timestamp(event_time) AS client_event_time
--         FROM bronze.amplitude_addi_funnel_home_store_tapped
--     )
--     UNION ALL
--     (
--         SELECT
--             'amplitude_addi_funnel_payments_pay_button_tapped' AS my_source,
--             'new_bronze' AS my_project,
--             event_type AS original_event_name,
--             session_id,
--             user_id AS client_id,
--             country,
--             to_date(event_time) AS client_event_date,
--             to_timestamp(event_time) AS client_event_time
--         FROM bronze.amplitude_addi_funnel_payments_pay_button_tapped
--     )
--     UNION ALL
--     (
--         SELECT
--             'amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched' AS my_source,
--             'new_bronze' AS my_project,
--             event_type AS original_event_name,
--             session_id,
--             user_id AS client_id,
--             country,
--             to_date(event_time) AS client_event_date,
--             to_timestamp(event_time) AS client_event_time
--         FROM bronze.amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched
--     ))
--     WHERE client_id is not NULL AND client_event_date >= '2022-06-03' --First day of data (03 and 05)
-- )
-- --/* -- TURNED OFF FOR THE MOMENT - VERIFICATION NEEDED
-- ,
new_app_data_engagement_silver AS (
SELECT 
    CONCAT('silver_event_type_',event_type) AS my_source,
    'new_silver' AS my_project,
    event_type AS original_event_name,
    session_id,
    user_id AS client_id,
    country,
    to_date(event_time) AS client_event_date,
    to_timestamp(event_time) AS client_event_time
FROM silver.f_amplitude_addi_funnel_project 
WHERE  event_type IN ('HOME_STORE_TAPPED','SHOP_STORE_TAPPED','PAYMENTS_PAY_BUTTON_TAPPED','PAYMENTS_BR_PIX_FORM_PAYNOW_BUTTON_TOUCHED') 
    AND user_id IS NOT NULL
)
--*/
,
frontend_events AS (
    SELECT * FROM legacy_app_data_engagement 
    UNION ALL
    --SELECT * FROM new_app_data_engagement_bronze 
    -- UNION ALL -- TURNED OFF FOR THE MOMENT - VERIFICATION NEEDED
    SELECT * FROM new_app_data_engagement_silver 
)
,

originations_br as (
    (
    SELECT 
          client_id,
          origination_date,
          application_id
      FROM silver.f_originations_bnpl_br 
    )
    
    UNION ALL
    (
    SELECT
          client_id,
          origination_date,
          application_id
    FROM silver.f_originations_bnpn_br 
    )
),

interest_originations_br as (
    SELECT 
          'BR' AS country_code,
          o.client_id,
          MIN(o.origination_date) AS min_origination_datetime_utc
      FROM silver.f_applications_br s
      RIGHT JOIN originations_br o
      ON s.application_id = o.application_id
      WHERE (lower(s.client_type) = 'client' OR lower(s.journey_name) LIKE '%pago%') -- GET THE MINIMUM ORIGINATION TIMESTAMP OF WHEN SOMEONE BECOMES A CLIENT AND GETS ACCESS TO THE APP
        -- AND lower(journey_name) NOT LIKE '%preapproval%' AND lower(application_channel) LIKE '%pre_approval%' -- Preapproval journeys are not included in this table scope.
      GROUP BY 1,2

)
,
interest_preapprovals_br (
    SELECT 
        'BR' AS country_code,
        client_id,
        MIN(ocurred_on) AS min_preapproval_datetime_utc

    FROM silver.f_origination_events_br_logs
    WHERE client_id IS NOT NULL AND event_name ='PreapprovalApplicationCompletedBR'
    GROUP BY 1,2
)
,
support_for_originations_co AS (
    SELECT 
        application_id,
        MAX(ocurred_on_date) AS approved_ocurred_on

    FROM silver.f_origination_events_co_logs  
    WHERE event_type  = 'APPROVAL' -- AND channel != 'PRE_APPROVAL'
    GROUP BY 1
)
,


originations_co as (
    SELECT 
          client_id,
          origination_date,
          application_id
      FROM silver.f_originations_bnpl_co  
),

applications_co as (

    SELECT 
          client_type,
          journey_name,
          application_id
      FROM silver.f_applications_co  
),

union_originations_applications as (

      select * except (a.application_id)
      from originations_co o
      left join applications_co a
      on o.application_id = a.application_id
),

interest_originations_co as (

   SELECT 
        'CO' AS country_code,
        io_co.client_id,
        MIN(COALESCE(sup_co.approved_ocurred_on, io_co.origination_date)) AS min_origination_datetime_utc

    FROM union_originations_applications AS io_co
    LEFT JOIN support_for_originations_co AS sup_co ON io_co.application_id = sup_co.application_id
    WHERE (lower(io_co.client_type) = 'client' or lower(io_co.journey_name) LIKE '%pago%' OR lower(io_co.journey_name) LIKE '%financia%') -- ONLY FOR clients that ARE now considered clients, that IS AFTER a FIRST successful BNPL TRANSACTION WITH addi
        AND lower(io_co.journey_name) NOT LIKE '%expedited%' AND lower(io_co.journey_name) NOT LIKE '%santander%'
        -- AND lower(io_co.journey_name) NOT LIKE '%preapproval%' AND lower(io_co.application_channel) LIKE '%pre_approval%' -- Preapproval journeys are not included in this table scope.
    GROUP BY 1,2
 ),


interest_preapprovals_co AS(
    SELECT 
        'CO' AS country_code,
        client_id,
        MIN(ocurred_on) AS min_preapproval_datetime_utc

    FROM silver.f_origination_events_co_logs  
    WHERE client_id IS NOT NULL AND event_name ='PreapprovalApplicationCompletedCO'
    GROUP BY 1,2
)
,
interest_prospects AS (
    (
        SELECT country_code, client_id, MIN(datetime_utc) AS min_datetime_utc
        FROM
        (
        SELECT country_code, client_id, min_origination_datetime_utc AS datetime_utc FROM interest_originations_br
        UNION ALL
        SELECT country_code, client_id, min_preapproval_datetime_utc AS datetime_utc FROM interest_preapprovals_br
        )
        GROUP BY 1,2
    )
    UNION ALL
    (
        SELECT country_code, client_id, MIN(datetime_utc) AS min_datetime_utc
        FROM
        (
        SELECT country_code, client_id, min_origination_datetime_utc AS datetime_utc FROM interest_originations_co
        UNION ALL
        SELECT country_code, client_id, min_preapproval_datetime_utc AS datetime_utc FROM interest_preapprovals_co
        )
        GROUP BY 1,2
    )
)


SELECT
    ip.country_code,
    fe_e.client_id,
    fe_e.client_event_time::timestamp AS event_ocurred_on_utc,
    CASE WHEN ip.country_code = 'BR' THEN from_utc_timestamp(fe_e.client_event_time,'America/Sao_Paulo')::timestamp
         WHEN ip.country_code = 'CO' THEN from_utc_timestamp(fe_e.client_event_time,'America/Bogota')::timestamp
         ELSE fe_e.client_event_time::timestamp 
    END AS event_ocurred_on_local,
    CASE WHEN ip.country_code = 'BR' THEN from_utc_timestamp(fe_e.client_event_time,'America/Sao_Paulo')::date
         WHEN ip.country_code = 'CO' THEN from_utc_timestamp(fe_e.client_event_time,'America/Bogota')::date
         ELSE fe_e.client_event_time::date
    END AS event_ocurred_on_date_local,
    ip.min_datetime_utc AS first_went_into_interest_group_at_utc,
    CONCAT(my_project,'__' ,my_source) AS event,
    fe_e.client_event_time > ip.min_datetime_utc AS quality_flag
    
FROM       frontend_events    AS fe_e
INNER JOIN interest_prospects AS ip   ON fe_e.client_id = ip.client_id
WHERE fe_e.client_id IS NOT NULL