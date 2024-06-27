


WITH deals_allies AS (

    SELECT
        properties_pipeline AS pipeline_id,
        pipelines.label AS pipeline_label,
        deals.properties_dealstage AS stage_id,
        stages.label AS stage_label,
        properties_dealname AS deal_name,
        properties_slug AS slug,
        properties_ally_slug AS ally_slug,
        properties_ally_slug1 AS ally_slug_1,
        properties_slug___paylink AS slug_paylink,
        properties_slug___e_commerce AS slug_e_commerce,
        properties_kam AS kam_id,
        properties_ally_brand AS ally_brand,
        properties_ally_tipe AS ally_tipe,
        properties_ally_type AS ally_type,
        properties_activacion_ AS activacion,
        properties_activation_complete_ AS activacion_complete,
        CASE
            WHEN properties_activation_complete_ = true THEN true
            ELSE false
        END AS activation_complete_bool,
        properties_activation_complete_date AS activation_complete_date,
        properties_activation_date AS activation_date,
        properties_activation_notes AS activation_notes,
        properties_activation_time AS activation_time,
        properties_active_stores AS activation_stores,
        properties_actividad_del_aliado AS actividad_aliado,
        properties_check_in_date_kyp AS checkin_date_kyp,
        properties_date_success_graduation AS succession_graduation_date,
        properties_closedate AS close_date,
        o.firstName AS owner_firstname,
        o.lastName AS owner_lastname,
        deals.updatedAt AS updated_at,
        row_number() OVER (PARTITION BY pipelines.label, deals.properties_dealname ORDER BY deals.updatedAt DESC) AS rn
        --case when properties_activacion_ = 'Sí' then true else false end as active_2
        --properties_activation_complete_,
        --archived,
        --properties_active_stores, -- sin data
        --properties_status___receita_federal

    FROM bronze.hubspot_deals_all deals

    LEFT JOIN bronze.hubspot_deals_stages_all AS stages
    ON stages.stageId = deals.properties_dealstage

    LEFT JOIN bronze.hubspot_deals_pipelines_all pipelines
    ON pipelines.pipelineId = deals.properties_pipeline

    LEFT JOIN bronze.hubspot_owners_all o
    ON deals.properties_kam = o.id )


SELECT pipeline_id,
       pipeline_label,
       stage_id,
       stage_label,
       deal_name,
       slug,
       ally_slug,
       ally_slug_1,
       slug_paylink,
       slug_e_commerce,
       kam_id,
       ally_brand,
       ally_tipe,
       ally_type,
       activacion,
       activacion_complete,
       activation_complete_bool,
       cast(activation_complete_date AS date) AS activation_complete_date,
       cast(activation_date AS date) AS activation_date,
       activation_notes,
       activation_time,
       activation_stores,
       actividad_aliado,
       cast(checkin_date_kyp AS date) AS checkin_date_kyp,
       cast(succession_graduation_date AS date) AS succession_graduation_date,
       cast(close_date AS timestamp) AS close_date,
       owner_firstname,
       owner_lastname,
       cast(updated_at AS timestamp) AS updated_at
FROM deals_allies
WHERE rn = 1 
AND stage_label IN ('Active.', 'Go Live', 'Farming')