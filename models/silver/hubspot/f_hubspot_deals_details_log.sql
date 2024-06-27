{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- Deprecado para optimizar query

-- WITH activation_owner as(
--   SELECT DISTINCT properties_owner_activacion,
--   concat(o.firstName,' ',o.lastName)  as activation_owner
--   FROM bronze.hubspot_deals_all deals
--   LEFT JOIN bronze.hubspot_owners_all o
--   ON deals.properties_owner_activacion=o.id
-- ),

-- deal_owner as (
--   SELECT DISTINCT properties_hubspot_owner_id,
--   concat(o.firstName,' ',o.lastName)  as deal_owner
--   FROM bronze.hubspot_deals_all deals
--   LEFT JOIN bronze.hubspot_owners_all o
--   ON deals.properties_hubspot_owner_id=o.id
-- )

SELECT DISTINCT
    properties_pipeline AS pipeline_id,
    pipelines.label AS pipeline_label,
    deals.properties_dealstage AS stage_id,
    stages.label AS stage_label,
    deals.id AS deal_id,
    properties_dealname AS deal_name,
    properties_producto AS product,
    properties_slug AS slug,
    properties_ally_slug AS ally_slug,
    properties_ally_slug1 AS ally_slug_1,
    properties_ally_brand AS ally_brand,
    properties_ally_type AS ally_cluster,
    properties_contrato_social_anexado_ AS contrato_social_anexado,
    properties_envio_material_pop AS pop_comments,
    properties_channel AS channel,
    properties_channel_new AS channel_new,
    --properties_ally_activity AS ally_activity,
    properties_payment_term AS payment_term,
    CAST(properties_closedate AS timestamp) AS close_date,
    properties_deal_currency_code AS currency,
    properties_ally_segmentation AS ally_segmentation,
    properties_minimum_amount AS minimum_amount,
    properties_formato_store_users__ecommerce_na_ AS store_user_format,
    properties_cancelation_mdf__ AS cancelation_mdf,
    properties_cancelation_mdf___new AS cancelation_mdf_new,
    properties_link_dashboard AS url_dashboard,
    properties_url AS url_ally,
    CONCAT('https://app.hubspot.com/contacts/5471282/deal/', deals.id) AS url_deal,
    properties_mdf AS current_mdf,
    deals.properties_hubspot_owner_id AS deal_owner_id,
    properties_gmv AS gmv,
    properties_ally_tipe AS ally_type,
    properties_fraud_mdf__ AS fraud_mdf,
    properties_platform AS platform,
    properties_cluster_type AS cluster_type,
    properties_accounting_contact AS accounting_contact,
    properties_vertical AS vertical,
    properties_adjunto_archivo_ AS adjunto_archivo,
    properties_kam AS kam_id,
    properties_maximum_amount AS maximum_amount,
    properties_amount AS amount,
    properties_installments AS installments,
    properties_documents_and_logo_ok_ AS documents_and_logo,
    properties_technical_contact AS technical_contact,
    properties_origination_mdf__ AS origination_mdf,
    properties_average_ticket AS average_ticket,
    deals.properties_estate as estate,
    CAST(properties_activation_complete_date AS date) AS activation_complete_date,
    CAST(properties_start_activation_date AS date) AS checkin_date_activation,
    CAST(properties_check_in AS date) AS checkin_date_data_review,
    CAST(properties_check_in_date___churned AS date) AS checkin_date_churned,
    CAST(properties_check_in_date___enabled AS date) AS checkin_date_go_live,
    CAST(properties_check_in_date___farming AS date) AS checkin_date_farming,
    CAST(properties_check_in_date___launch_campaign AS date) AS checkin_date_launch_campaign,
    CAST(properties_check_in_date___lost AS date) AS checkin_date_lost,
    CAST(properties_check_in_date___mql AS date) AS checkin_date_mql,
    CAST(properties_check_in_date___mql_not_approved AS date) AS checkin_date_mql_not_approved,
    CAST(properties_check_in_date___negotiation AS date) AS checkin_date_negotiation,
    CAST(properties_check_in_date___offer_accepted AS date) AS checkin_date_offer_accepted,
    CAST(properties_check_in_date___on_hold AS date) AS checkin_date_on_hold,
    CAST(properties_check_in_date___poc AS date) AS checkin_date_poc,
    CAST(properties_check_in_date___prospect AS date) AS checkin_date_prospect,
    CAST(properties_check_in_date___ramp_up AS date) AS checkin_date_ramp_up,
    CAST(properties_check_in_date___sales_pitch AS date) AS checkin_date_sales_pitch,
    CAST(properties_check_in_date___sql_business_validated AS date) AS checkin_date_sql_business_validated,
    CAST(properties_check_in_date___success AS date) AS checkin_date_success,
    CAST(properties_check_in_date_kyp AS date) AS checkin_date_kyp,
    CAST(properties_onboarding_completion_date AS date) AS checkin_training,
    CAST(properties_date_success_graduation AS date) AS succession_graduation_date,
    properties_createdate AS create_date,
    properties_termination_date AS termination_date,
    properties_cargo_los_datos_tecnicos_ AS cargo_datos_tecnicos,
    properties_number AS number,
    properties_city AS city,
    properties_cep AS cep,
    properties_district AS district,
    properties_bnpn_ AS bnpn,
    properties_slug___paylink AS slug_paylink,
    properties_slug___e_commerce AS slug_e_commerce,
    properties_physical_address AS address,
    o.firstName AS owner_firstname,
    o.lastName AS owner_lastname,
    -- ao.activation_owner,
    -- do.deal_owner,
    CAST(deals.updatedAt AS timestamp) AS updated_at  
    FROM {{ source('bronze', 'hubspot_deals_all') }} deals

    LEFT JOIN {{ source('bronze', 'hubspot_deals_stages_all') }} AS stages
        ON stages.stageId = deals.properties_dealstage

    LEFT JOIN {{ source('bronze', 'hubspot_deals_pipelines_all') }} pipelines
        ON pipelines.pipelineId = deals.properties_pipeline

    LEFT JOIN {{ source('bronze', 'hubspot_owners_all') }} o
        ON deals.properties_kam = o.id

    --     Deprecado para optimizar query
    -- LEFT JOIN activation_owner ao
    --     ON deals.properties_owner_activacion=ao.properties_owner_activacion

    -- LEFT JOIN deal_owner do
    --     ON deals.properties_hubspot_owner_id = do.properties_hubspot_owner_id
