{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH legacy_app_data_attribution_model AS (
    (select 
        'app_home_specific_store_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         event_id,
         null as unique_id,
         client_id,
         country,
         store_id AS ally_name,
         _dt AS client_event_date,
         client_event_time,
         NULL AS component_name, 
         NULL AS screen_name
    from hive_metastore.tru.app_home_specific_store_touched
    where client_id is not NULL AND store_id IS NOT NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL
    (
    select 
        'app_store_specific_touched' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         event_id,
         null as unique_id,
         client_id,
         country,
         store_id AS ally_name,
         _dt AS client_event_date,
         client_event_time,
         NULL AS component_name, 
         NULL AS screen_name
    from hive_metastore.tru.app_store_specific_touched
    where client_id is not NULL AND store_id IS NOT NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
    UNION ALL 
    (
    select 
        'app_store_tapped' AS my_source,
         'legacy_tru' AS my_project,
         event_type AS original_event_name,
         session_id,
         event_id,
         null as unique_id,
         client_id,
         country,
         COALESCE(ally_name, store_id) AS ally_name,
         _dt AS client_event_date,
         client_event_time,
         CASE WHEN origin ilike '%category' THEN 'CATEGORY'
							WHEN origin ilike '%home%' THEN 'HOME'
							WHEN origin ilike '%stores' THEN 'STORES'
							WHEN origin ilike '%shop now' THEN 'SHOP_NPW'
							WHEN origin ilike '%discounts' THEN 'DISCOUNTS'
							WHEN origin ilike '%preferred%' THEN 'PREFERRED'
							WHEN origin ilike '%shop_now' THEN 'SHOP_NOW'
							ELSE origin 
         END as screen_name,
         origin as component_name
    from hive_metastore.tru.app_store_tapped
    where client_id is not NULL AND COALESCE(ally_name, store_id) IS NOT NULL AND to_date(_dt) <= '2022-07-14' --Last day of data
    )
)
,

new_app_data_attribution_model_silver AS (
SELECT 
    CONCAT('silver_event_type_',event_type) AS my_source,
    'new_silver' AS my_project,
    event_type AS original_event_name,
    session_id,
    event_id,
    unique_id,
    user_id AS client_id,
    country,
     COALESCE(a.ally_name, a.ally_slug, a.event_properties:['allySlug']) AS ally_name,
    to_date(event_time) AS client_event_date,
    to_timestamp(event_time) AS client_event_time,
    event_properties:componentName AS component_name, 
    event_properties:screenName AS screen_name
FROM {{ source('silver', 'f_amplitude_addi_funnel_project') }} a
WHERE event_type IN ('HOME_STORE_TAPPED','SHOP_STORE_TAPPED','SELECT_STORE','SELECT_DEAL') AND user_id IS NOT NULL AND ally_name IS NOT NULL AND to_date(event_time) >= '2022-06-03'
)
,
frontend_events AS (
    SELECT * FROM legacy_app_data_attribution_model 
    UNION ALL
    SELECT * FROM new_app_data_attribution_model_silver 
)
,

originations_br as (
    (
    SELECT
        origination_date as origination_datetime,
        to_date(from_utc_timestamp(origination_date,'America/Sao_Paulo')) AS origination_date_local,
        client_id,
        application_id,
        loan_id
    FROM {{ source('silver', 'f_originations_bnpl_br') }}
    )
    UNION ALL
    (
    SELECT
        origination_date as origination_datetime,
        to_date(from_utc_timestamp(origination_date,'America/Sao_Paulo')) AS origination_date_local,
        client_id,
        application_id,
        null as loan_id
    FROM {{ source('silver', 'f_originations_bnpn_br') }}
    )
),

interest_originations_br as (
    SELECT
        'BR' AS country_code,
        ally_slug,
        application_br.client_id,
        application_br.application_id,
        client_type,
        journey_name,
        channel,
        product,
        originations_br.loan_id,
        originations_br.origination_datetime,
        originations_br.origination_date_local
    FROM {{ source('silver', 'f_applications_br') }} as application_br
    JOIN originations_br ON originations_br.application_id = application_br.application_id
    WHERE lower(client_type) = 'client' -- ONLY FOR clients that ARE now considered clients, that IS AFTER a FIRST successful BNPL TRANSACTION WITH addi
        AND lower(channel) LIKE '%e_commerce%' -- ONLY Ecommerce
),

support_for_originations_co( 
    SELECT 
        application_id,
        MAX(ocurred_on_date) AS approved_ocurred_on
    FROM {{ source('silver', 'f_origination_events_co_logs') }} 
    WHERE event_type  = 'APPROVAL' -- AND channel != 'PRE_APPROVAL'
    GROUP BY 1
)
, -- NEEDS A SUPPORT TABLE BECAUSE THE ORIGINATION TIMESTAMP IS NOT FROM THE APPROVAL EVENT AS IT SHOULD BE, OR AS IT IS IN BR
originations_co as (
    SELECT
        o_bnpl_co.origination_date as origination_datetime,
        COALESCE(sup_co.approved_ocurred_on, to_date(from_utc_timestamp(o_bnpl_co.origination_date,'America/Bogota'))) AS origination_date_local,
        o_bnpl_co.client_id,
        o_bnpl_co.application_id,
        o_bnpl_co.loan_id
    FROM {{ source('silver', 'f_originations_bnpl_co') }} as o_bnpl_co
    LEFT JOIN support_for_originations_co AS sup_co ON o_bnpl_co.application_id = sup_co.application_id
),

interest_originations_co as (
    SELECT
        'CO' AS country_code,
        io_co.ally_slug,
        io_co.client_id,
        io_co.application_id,
        io_co.client_type,
        io_co.journey_name,
        io_co.channel,
        io_co.product,
        originations_co.loan_id,
        originations_co.origination_datetime,
        originations_co.origination_date_local
    FROM {{ source('silver', 'f_applications_co') }} AS io_co
    JOIN originations_co ON originations_co.application_id = io_co.application_id
    WHERE lower(io_co.client_type) = 'client' -- ONLY FOR clients that ARE now considered clients, that IS AFTER a FIRST successful BNPL TRANSACTION WITH addi
        AND lower(io_co.channel) LIKE '%e_commerce%' -- ONLY Ecommerce
        -- AND lower(io_co.journey_name) NOT LIKE '%preapproval%' AND lower(io_co.application_channel) LIKE '%pre_approval%' -- Preapproval journeys are not included in this table scope.
)
,
loans_meeting_criteria_7days(
    SELECT io.loan_id,
           1 as app_purchase_criteria_7days,
           COLLECT_SET(named_struct('source',ost.my_source,
                                    'dt_utc',ost.client_event_time,
                                    'screen', ost.screen_name,
                                    'component', ost.component_name,
                                    'session_id', ost.session_id ,
                                    'event_id', ost.event_id,
                                    'unique_id', ost.unique_id)) AS sources_7days,
           MIN(ost.client_event_time)::timestamp AS min_event_ocurred_on_utc_in_window_7days,
           MAX(ost.client_event_time)::timestamp AS max_event_ocurred_on_utc_in_window_7days
    from (
        SELECT * FROM interest_originations_br
        UNION ALL
        SELECT * FROM interest_originations_co
    )                                  AS io
    left join frontend_events          AS ost ON io.client_id = ost.client_id
                                            AND io.ally_slug = ost.ally_name
    -- If the person clicks on a store and makes a purchase after 7 days it counts as app purchase
    where ost.client_event_time < io.origination_datetime -- APP event timestamp BEFORE origination timestamp
        and to_date(ost.client_event_time) >= (to_date(io.origination_datetime) - interval 7 day) -- APP event IN INTERVAL 
    GROUP BY 1,2
)
,
loans_meeting_criteria_2days(
     SELECT io.loan_id,
           1 as app_purchase_criteria_2days,
          COLLECT_SET(named_struct('source',ost.my_source,
                                    'dt_utc',ost.client_event_time,
                                    'screen',ost.screen_name,
                                    'component',ost.component_name,
                                    'session_id', ost.session_id ,
                                    'event_id', ost.event_id,
                                    'unique_id', ost.unique_id)) AS sources_2days,
           MIN(ost.client_event_time)::timestamp AS min_event_ocurred_on_utc_in_window_2days,
           MAX(ost.client_event_time)::timestamp AS max_event_ocurred_on_utc_in_window_2days
    from (
        SELECT * FROM interest_originations_br
        UNION ALL
        SELECT * FROM interest_originations_co
    )                                  AS io
    left join frontend_events          AS ost ON io.client_id = ost.client_id
                                            AND io.ally_slug = ost.ally_name
    -- If the person clicks on a store and makes a purchase after 2 days it counts as app purchase
    where ost.client_event_time < io.origination_datetime -- APP event timestamp BEFORE origination timestamp
        and to_date(ost.client_event_time) >= (to_date(io.origination_datetime) - interval 2 day) -- APP event IN INTERVAL 
    GROUP BY 1,2
)
--, results AS (
    SELECT io.*,
           COALESCE(lmc7.app_purchase_criteria_7days,0) AS app_purchase_criteria_7days,
           lmc7.sources_7days,
           lmc7.min_event_ocurred_on_utc_in_window_7days,
           lmc7.max_event_ocurred_on_utc_in_window_7days,
           COALESCE(lmc2.app_purchase_criteria_2days,0) AS app_purchase_criteria_2days,
           lmc2.sources_2days,
           lmc2.min_event_ocurred_on_utc_in_window_2days,
           lmc2.max_event_ocurred_on_utc_in_window_2days,
           NOW() AS ingested_at,
           to_timestamp('{{ var("execution_date") }}') AS updated_at
    FROM (
        SELECT * FROM interest_originations_br
        UNION ALL
        SELECT * FROM interest_originations_co
    ) AS io 
    LEFT JOIN loans_meeting_criteria_7days AS lmc7 ON io.loan_id = lmc7.loan_id
    LEFT JOIN loans_meeting_criteria_2days AS lmc2 ON io.loan_id = lmc2.loan_id
    WHERE io.origination_date_local >= '2021-10-25'
--)
