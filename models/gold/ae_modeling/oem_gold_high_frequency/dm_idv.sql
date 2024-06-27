{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH co_identitywastarted AS (
    WITH idv_data_step_1 AS (
        SELECT
            app.application_id,
            app.client_id,
            from_utc_timestamp(app.application_date, 'America/Bogota') AS application_date,
            app.client_type,
            app.journey_name,
            app.product AS product,
            idv.agent_user_id,
            idv.used_policy_id,
            idv.reason,
            idv.observations,
            from_utc_timestamp(idv.identitywastarted_at, 'America/Bogota') AS identitywastarted_at,
            from_utc_timestamp(idv.identitywaapproved_at, 'America/Bogota') AS identitywaapproved_at,
            from_utc_timestamp(idv.identitywarejected_at, 'America/Bogota') AS identitywarejected_at,
            from_utc_timestamp(idv.identitywadiscarded_at, 'America/Bogota') AS identitywadiscarded_at,
            from_utc_timestamp(idv.identitywadiscardedbyrisk_at, 'America/Bogota') AS identitywadiscardedbyrisk_at,
            rmt.credit_policy_name,
            rmt.term,
            rmt.approved_amount,
            rmt.requested_amount,
            rmt.ally_name,
            CASE
                WHEN idv.identitywaapproved_at IS NOT NULL THEN 'approved'
                WHEN idv.identitywarejected_at IS NOT NULL THEN 'rejected'
                WHEN (idv.identitywadiscarded_at IS NOT NULL or idv.identitywadiscardedbyrisk_at IS NOT NULL) THEN 'discarded'
                ELSE 'pending'
            END AS idv_status_new
        FROM {{ ref('f_applications_co') }} app
        LEFT JOIN {{ ref('f_idv_stage_co') }} idv                         ON app.application_id = idv.application_id
        LEFT JOIN {{ ref('risk_master_table_co') }} rmt                     ON app.application_id = rmt.application_id
        WHERE idv.identitywastarted_at IS NOT NULL
            AND app.client_type = 'PROSPECT'
    )
    ,
    underwiting_stages AS (
        SELECT
            DISTINCT application_id
        FROM {{ ref('f_origination_events_co_logs') }}
        WHERE journey_stage_name ilike '%underwriting-co%' OR journey_stage_name ilike '%underwriting-psychometric-co%'
            AND journey_name ilike '%SANTANDER%'
    )
    ,
    idv_data_step_2 AS (
        SELECT
            to_date(idv_1.application_date) AS period,
            idv_1.application_date,
            idv_1.used_policy_id AS idv_policy,
            lower(idv_1.reason) AS reason,
            CASE
                WHEN idv_1.journey_name like '%SANTANDER%' and udw.application_id IS NOT NULL THEN 'FINANCIA_CO'
                WHEN idv_1.journey_name like '%SANTANDER%' THEN 'SANTANDER_CO'
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%PAGO%' THEN 'PAGO_CO'
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%FINANCIA%' THEN 'FINANCIA_CO'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%PAGO%' THEN 'PAGO_CO'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%FINANCIA%' THEN 'FINANCIA_CO'
                WHEN idv_1.credit_policy_name in (
                  'addipago_0aprfga_policy',
                  'addipago_0fga_policy',
                  'addipago_claro_policy',
                  'addipago_mario_h_policy',
                  'addipago_no_history_policy',
                  'addipago_policy',
                  'addipago_policy_amoblando',
                  'adelante_policy_pago',
                  'closing_policy_pago',
                  'default_policy_pago',
                  'finalization_policy_pago',
                  'rc_0aprfga',
                  'rc_0fga',
                  'rc_addipago_policy_amoblando',
                  'rc_adelante_policy',
                  'rc_closing_policy',
                  'rc_finalization_policy',
                  'rc_pago_0aprfga',
                  'rc_pago_0fga',
                  'rc_pago_claro',
                  'rc_pago_mario_h',
                  'rc_pago_standard',
                  'rc_rejection_policy',
                  'rc_standard',
                  'rejection_policy_pago') THEN 'PAGO_CO'
                WHEN idv_1.term > 3 THEN 'FINANCIA_CO'
                WHEN idv_1.credit_policy_name in ('closing_policy','finalization_policy', 'rejection_policy') and idv_1.term = 3 THEN 'PAGO_CO'
                WHEN idv_1.credit_policy_name IS null and idv_1.term > 3 THEN 'FINANCIA_CO'
                WHEN idv_1.credit_policy_name IS null and idv_1.approved_amount <= 600000 THEN 'PAGO_CO'
                WHEN idv_1.credit_policy_name IS NOT NULL THEN 'FINANCIA_CO' ELSE 'FINANCIA_CO'
            END AS channel,
            idv_1.application_id,
            idv_1.ally_name AS ally_id,
            lower(idv_1.ally_name) AS ally,
            idv_1.client_id AS prospect_id,
            idv_1.idv_status_new AS idv_status,
            idv_1.identitywastarted_at,
            idv_1.identitywaapproved_at,
            idv_1.identitywarejected_at,
            idv_1.identitywadiscarded_at,
            idv_1.identitywadiscardedbyrisk_at,
            CASE WHEN idv_1.idv_status_new = 'approved' THEN 1 ELSE 0 END AS is_approved,
            CASE WHEN idv_1.idv_status_new = 'rejected' THEN 1 ELSE 0 END AS is_rejected,
            CASE WHEN idv_1.idv_status_new = 'discarded' THEN 1 ELSE 0 END AS is_discarded,
            CASE WHEN idv_1.idv_status_new = 'pending' THEN 1 ELSE 0 END AS is_pending,
            idv_1.requested_amount AS ticket,
            idv_1.observations,
            u.email AS analyst,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(HOUR FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(HOUR FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' AND idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(HOUR FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(MINUTE FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(MINUTE FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' AND idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(MINUTE FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(SECOND FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(SECOND FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' AND idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(SECOND FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2) AS aht_hours_rejected,
            CASE WHEN idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(HOUR FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2) END AS aht_hours_discarded,
            round(extract(MINUTE FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2) AS aht_minutes_rejected,
            CASE WHEN idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(MINUTE FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2) END AS aht_minutes_discarded,
            round(extract(SECOND FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2) AS aht_seconds_rejected,
            CASE WHEN idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(SECOND FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2) END AS aht_seconds_discarded
        FROM idv_data_step_1 idv_1 LEFT JOIN bronze.identity_management_users_co u ON u.id = idv_1.agent_user_id
        LEFT JOIN underwiting_stages udw                                           ON udw.application_id = idv_1.application_id
    )
    SELECT
        period,
        'CO' as country_code,
        'idv_2.0' as idv_version,
        application_date,
        application_id,
        idv_status,
        identitywastarted_at,
        identitywaapproved_at,
        identitywarejected_at,
        identitywadiscarded_at,
        NULL AS identityphotosevaluationstarted_at,
        NULL AS identityphotosagentassigned_at,
        analyst,
        channel,
        idv_policy,
        reason,
        ally_id,
        ally,
        prospect_id,
        ticket,
        observations,
        NULL AS photo_quality,
        count(DISTINCT application_id) AS total_apps,
        count(DISTINCT analyst) AS active_analyst,
        sum(is_approved) AS idv_approved,
        sum(is_rejected) AS idv_rejected,
        sum(is_discarded) AS idv_discarded,
        sum(is_pending) AS idv_pending,
        round(percentile(aht_hours * 3600 + aht_minutes * 60 + aht_seconds, 0.5), 2) AS aht,
        round(percentile(aht_hours_approval * 3600 + aht_minutes_approval * 60 + aht_seconds_approval, 0.5), 2) AS aht_approval,
        round(percentile(aht_hours_rejected * 3600 + aht_minutes_rejected * 60 + aht_seconds_rejected, 0.5), 2) AS aht_rejected,
        round(percentile(aht_hours_discarded * 3600 + aht_minutes_discarded * 60 + aht_seconds_discarded, 0.5), 2) AS aht_discarded,
        NULL AS aht_handling,
        NULL AS aht_queued,
        NULL AS aht_queued_addi
    FROM idv_data_step_2
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,
co_identityphotosstarted AS (
    WITH idv_data_step_1 AS (
        SELECT
            app.application_id,
            app.client_id,
            from_utc_timestamp(app.application_date, 'America/Bogota') AS application_date,
            app.client_type,
            app.journey_name,
            app.product AS product,
            idv.agent_user_id,
            idv.used_policy_id,
            idv.reason,
            idv.observations,
            from_utc_timestamp(idv.identityphotosstarted_at, 'America/Bogota') AS identityphotosstarted_at,
            from_utc_timestamp(idv.identityphotosapproved_at, 'America/Bogota') AS identityphotosapproved_at,
            from_utc_timestamp(idv.identityphotosrejected_at, 'America/Bogota') AS identityphotosrejected_at,
            from_utc_timestamp(idv.identityphotosdiscarded_at, 'America/Bogota') AS identityphotosdiscarded_at,
            from_utc_timestamp(idv.identityphotosevaluationstarted_at, 'America/Bogota') AS identityphotosevaluationstarted_at,
            from_utc_timestamp(idv.identityphotosagentassigned_at, 'America/Bogota') AS identityphotosagentassigned_at,
            from_utc_timestamp(idv.identityphotosdiscardedbyrisk_at, 'America/Bogota') AS identityphotosdiscardedbyrisk_at,
            rmt.credit_policy_name,
            rmt.term,
            rmt.approved_amount,
            rmt.requested_amount,
            rmt.ally_name,
            idv.photo_quality,
            CASE
                WHEN idv.identityphotosapproved_at IS NOT NULL THEN 'approved'
                WHEN idv.identityphotosrejected_at IS NOT NULL THEN 'rejected'
                WHEN (idv.identityphotosdiscarded_at IS NOT NULL or idv.identityphotosdiscardedbyrisk_at IS NOT NULL) THEN 'discarded'
                ELSE 'pending'
            END AS idv_status_new
        FROM {{ ref('f_applications_co') }} app
        LEFT JOIN {{ ref('f_idv_stage_co') }} idv                         ON app.application_id = idv.application_id
        LEFT JOIN {{ ref('risk_master_table_co') }} rmt                     ON app.application_id = rmt.application_id
        WHERE idv.identityphotosstarted_at IS NOT NULL
    )
    ,
    underwiting_stages AS (
        SELECT
            DISTINCT application_id
        FROM {{ ref('f_origination_events_co_logs') }}
        WHERE journey_stage_name ilike '%underwriting-co%' OR journey_stage_name ilike '%underwriting-psychometric-co%'
            AND journey_name ilike '%SANTANDER%'
    )
    ,
    idv_data_step_2 AS (
        SELECT
            to_date(idv_1.application_date) AS period,
            idv_1.application_date,
            idv_1.used_policy_id AS idv_policy,
            lower(idv_1.reason) AS reason,
            CASE
                WHEN idv_1.journey_name like '%SANTANDER%' and udw.application_id IS NOT NULL THEN 'FINANCIA_CO'
                WHEN idv_1.journey_name like '%SANTANDER%' THEN 'SANTANDER_CO'
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%PAGO%' THEN 'PAGO_CO'
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%FINANCIA%' THEN 'FINANCIA_CO'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%PAGO%' THEN 'PAGO_CO'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%FINANCIA%' THEN 'FINANCIA_CO'
                WHEN idv_1.credit_policy_name in (
                  'addipago_0aprfga_policy',
                  'addipago_0fga_policy',
                  'addipago_claro_policy',
                  'addipago_mario_h_policy',
                  'addipago_no_history_policy',
                  'addipago_policy',
                  'addipago_policy_amoblando',
                  'adelante_policy_pago',
                  'closing_policy_pago',
                  'default_policy_pago',
                  'finalization_policy_pago',
                  'rc_0aprfga',
                  'rc_0fga',
                  'rc_addipago_policy_amoblando',
                  'rc_adelante_policy',
                  'rc_closing_policy',
                  'rc_finalization_policy',
                  'rc_pago_0aprfga',
                  'rc_pago_0fga',
                  'rc_pago_claro',
                  'rc_pago_mario_h',
                  'rc_pago_standard',
                  'rc_rejection_policy',
                  'rc_standard',
                  'rejection_policy_pago') THEN 'PAGO_CO'
                WHEN idv_1.term > 3 THEN 'FINANCIA_CO'
                WHEN idv_1.credit_policy_name in ('closing_policy','finalization_policy', 'rejection_policy') and idv_1.term = 3 THEN 'PAGO_CO'
                WHEN idv_1.credit_policy_name IS null and idv_1.term > 3 THEN 'FINANCIA_CO'
                WHEN idv_1.credit_policy_name IS null and idv_1.approved_amount <= 600000 THEN 'PAGO_CO'
                WHEN idv_1.credit_policy_name IS NOT NULL THEN 'FINANCIA_CO' ELSE 'FINANCIA_CO'
            END AS channel,
            idv_1.application_id,
            idv_1.ally_name AS ally_id,
            lower(idv_1.ally_name) AS ally,
            idv_1.client_id AS prospect_id,
            idv_1.idv_status_new AS idv_status,
            idv_1.identityphotosstarted_at AS identitywastarted_at,
            idv_1.identityphotosapproved_at AS identitywaapproved_at,
            idv_1.identityphotosrejected_at AS identitywarejected_at,
            idv_1.identityphotosdiscarded_at AS identitywadiscarded_at,
            idv_1.identityphotosevaluationstarted_at,
            idv_1.identityphotosagentassigned_at,
            CASE WHEN idv_1.idv_status_new = 'approved' THEN 1 ELSE 0 END AS is_approved,
            CASE WHEN idv_1.idv_status_new = 'rejected' THEN 1 ELSE 0 END AS is_rejected,
            CASE WHEN idv_1.idv_status_new = 'discarded' THEN 1 ELSE 0 END AS is_discarded,
            CASE WHEN idv_1.idv_status_new = 'pending' THEN 1 ELSE 0 END AS is_pending,
            idv_1.requested_amount AS ticket,
            idv_1.observations,
            u.email AS analyst,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(HOUR FROM (idv_1.identityphotosapproved_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(HOUR FROM (idv_1.identityphotosrejected_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' THEN round(extract(HOUR FROM (idv_1.identityphotosdiscarded_at - idv_1.identityphotosstarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(MINUTE FROM (idv_1.identityphotosapproved_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(MINUTE FROM (idv_1.identityphotosrejected_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' THEN round(extract(MINUTE FROM (idv_1.identityphotosdiscarded_at - idv_1.identityphotosstarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(SECOND FROM (idv_1.identityphotosapproved_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(SECOND FROM (idv_1.identityphotosrejected_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' THEN round(extract(SECOND FROM (idv_1.identityphotosdiscarded_at - idv_1.identityphotosstarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idv_1.identityphotosapproved_at-idv_1.identityphotosstarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idv_1.identityphotosrejected_at-idv_1.identityphotosstarted_at)),2) AS aht_hours_rejected,
            round(extract(HOUR FROM (idv_1.identityphotosdiscarded_at-idv_1.identityphotosstarted_at)),2) AS aht_hours_discarded,
            round(extract(HOUR FROM (coalesce(idv_1.identityphotosapproved_at,idv_1.identityphotosrejected_at,idv_1.identityphotosdiscarded_at,idv_1.identityphotosdiscardedbyrisk_at) - idv_1.identityphotosagentassigned_at)),2) AS aht_hours_handling,
            round(extract(HOUR FROM (idv_1.identityphotosevaluationstarted_at - idv_1.identityphotosstarted_at)),2) AS aht_hours_queued,
            round(extract(HOUR FROM (idv_1.identityphotosagentassigned_at - idv_1.identityphotosevaluationstarted_at)),2) AS aht_hours_queued_addi,
            round(extract(MINUTE FROM (idv_1.identityphotosapproved_at-idv_1.identityphotosstarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idv_1.identityphotosrejected_at-idv_1.identityphotosstarted_at)),2) AS aht_minutes_rejected,
            round(extract(MINUTE FROM (idv_1.identityphotosdiscarded_at-idv_1.identityphotosstarted_at)),2) AS aht_minutes_discarded,
            round(extract(MINUTE FROM (coalesce(idv_1.identityphotosapproved_at,idv_1.identityphotosrejected_at,idv_1.identityphotosdiscarded_at,idv_1.identityphotosdiscardedbyrisk_at) - idv_1.identityphotosagentassigned_at)),2) AS aht_minutes_handling,
            round(extract(MINUTE FROM (idv_1.identityphotosevaluationstarted_at - idv_1.identityphotosstarted_at)),2) AS aht_minutes_queued,
            round(extract(MINUTE FROM (idv_1.identityphotosagentassigned_at - idv_1.identityphotosevaluationstarted_at)),2) AS aht_minutes_queued_addi,
            round(extract(SECOND FROM (idv_1.identityphotosapproved_at-idv_1.identityphotosstarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idv_1.identityphotosrejected_at-idv_1.identityphotosstarted_at)),2) AS aht_seconds_rejected,
            round(extract(SECOND FROM (idv_1.identityphotosdiscarded_at-idv_1.identityphotosstarted_at)),2) AS aht_seconds_discarded,
            round(extract(SECOND FROM (coalesce(idv_1.identityphotosapproved_at,idv_1.identityphotosrejected_at,idv_1.identityphotosdiscarded_at,idv_1.identityphotosdiscardedbyrisk_at) - idv_1.identityphotosagentassigned_at)),2) AS aht_seconds_handling,
            round(extract(SECOND FROM (idv_1.identityphotosevaluationstarted_at - idv_1.identityphotosstarted_at)),2) AS aht_seconds_queued,
            round(extract(SECOND FROM (idv_1.identityphotosagentassigned_at - idv_1.identityphotosevaluationstarted_at)),2) AS aht_seconds_queued_addi,
            idv_1.photo_quality
        FROM idv_data_step_1 idv_1 LEFT JOIN bronze.identity_management_users_co u ON u.id = idv_1.agent_user_id
        LEFT JOIN underwiting_stages udw                                           ON udw.application_id = idv_1.application_id
    )
    SELECT
        period,
        'CO' as country_code,
        'idv_4.0' as idv_version,
        application_date,
        application_id,
        idv_status,
        identitywastarted_at,
        identitywaapproved_at,
        identitywarejected_at,
        identitywadiscarded_at,
        identityphotosevaluationstarted_at,
        identityphotosagentassigned_at,
        analyst,
        channel,
        idv_policy,
        reason,
        ally_id,
        ally,
        prospect_id,
        ticket,
        observations,
        photo_quality,
        count(DISTINCT application_id) AS total_apps,
        count(DISTINCT analyst) AS active_analyst,
        sum(is_approved) AS idv_approved,
        sum(is_rejected) AS idv_rejected,
        sum(is_discarded) AS idv_discarded,
        sum(is_pending) AS idv_pending,
        round(percentile(aht_hours * 3600 + aht_minutes * 60 + aht_seconds, 0.5), 2) AS aht,
        round(percentile(aht_hours_approval * 3600 + aht_minutes_approval * 60 + aht_seconds_approval, 0.5), 2) AS aht_approval,
        round(percentile(aht_hours_rejected * 3600 + aht_minutes_rejected * 60 + aht_seconds_rejected, 0.5), 2) AS aht_rejected,
        round(percentile(aht_hours_discarded * 3600 + aht_minutes_discarded * 60 + aht_seconds_discarded, 0.5), 2) AS aht_discarded,
        round(percentile(aht_hours_handling * 3600 + aht_minutes_handling * 60 + aht_seconds_handling, 0.5), 2) AS aht_handling,
        round(percentile(aht_hours_queued * 3600 + aht_minutes_queued * 60 + aht_seconds_queued, 0.5), 2) AS aht_queued,
        round(percentile(aht_hours_queued_addi * 3600 + aht_minutes_queued_addi * 60 + aht_seconds_queued_addi, 0.5), 2) AS aht_queued_addi
    FROM idv_data_step_2
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,
br_identitywastarted AS (
    WITH idv_data_step_1 AS (
        SELECT
            app.application_id,
            app.client_id,
            from_utc_timestamp(app.application_date, 'America/Sao_Paulo') AS application_date,
            app.client_type,
            app.journey_name,
            app.product AS product,
            idv.agent_user_id,
            idv.used_policy_id,
            idv.reason,
            idv.observations,
            from_utc_timestamp(idv.identitywastarted_at, 'America/Sao_Paulo') AS identitywastarted_at,
            from_utc_timestamp(idv.identitywaapproved_at, 'America/Sao_Paulo') AS identitywaapproved_at,
            from_utc_timestamp(idv.identitywarejected_at, 'America/Sao_Paulo') AS identitywarejected_at,
            from_utc_timestamp(idv.identitywadiscarded_at, 'America/Sao_Paulo') AS identitywadiscarded_at,
            from_utc_timestamp(idv.identitywadiscardedbyrisk_at, 'America/Sao_Paulo') AS identitywadiscardedbyrisk_at,
            rmt.credit_policy_name,
            rmt.term,
            rmt.approved_amount,
            rmt.requested_amount,
            rmt.ally_name,
            CASE
                WHEN idv.identitywaapproved_at IS NOT NULL THEN 'approved'
                WHEN idv.identitywarejected_at IS NOT NULL THEN 'rejected'
                WHEN (idv.identitywadiscarded_at IS NOT NULL or idv.identitywadiscardedbyrisk_at IS NOT NULL) THEN 'discarded'
                ELSE 'pending'
            END AS idv_status_new
        FROM {{ ref('f_applications_br') }} app
        LEFT JOIN {{ ref('f_idv_stage_br') }} idv                         ON app.application_id = idv.application_id
        LEFT JOIN {{ ref('risk_master_table_br') }} rmt                     ON app.application_id = rmt.application_id
        WHERE idv.identitywastarted_at IS NOT NULL
            AND app.client_type = 'PROSPECT'
    )
    ,
    underwiting_stages AS (
        SELECT
            DISTINCT application_id
        FROM {{ ref('f_origination_events_br_logs') }}
        WHERE journey_stage_name ilike '%underwriting-br%' OR journey_stage_name ilike '%underwriting-psychometric-br%'
    )
    ,
    idv_data_step_2 AS (
        SELECT
            to_date(idv_1.application_date) AS period,
            idv_1.application_date,
            idv_1.used_policy_id AS idv_policy,
            lower(idv_1.reason) AS reason,
            CASE
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%PAGO%' THEN 'PAGO_BR'
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%FINANCIA%' THEN 'FINANCIA_BR'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%PAGO%' THEN 'PAGO_BR'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%FINANCIA%' THEN 'FINANCIA_BR'
                WHEN idv_1.credit_policy_name in (
                  'addipago_0aprfga_policy',
                  'addipago_0fga_policy',
                  'addipago_claro_policy',
                  'addipago_mario_h_policy',
                  'addipago_no_history_policy',
                  'addipago_policy',
                  'addipago_policy_amoblando',
                  'adelante_policy_pago',
                  'closing_policy_pago',
                  'default_policy_pago',
                  'finalization_policy_pago',
                  'rc_0aprfga',
                  'rc_0fga',
                  'rc_addipago_policy_amoblando',
                  'rc_adelante_policy',
                  'rc_closing_policy',
                  'rc_finalization_policy',
                  'rc_pago_0aprfga',
                  'rc_pago_0fga',
                  'rc_pago_claro',
                  'rc_pago_mario_h',
                  'rc_pago_standard',
                  'rc_rejection_policy',
                  'rc_standard',
                  'rejection_policy_pago') THEN 'PAGO_BR'
                WHEN idv_1.term > 3 THEN 'FINANCIA_BR'
                WHEN idv_1.credit_policy_name in ('closing_policy','finalization_policy', 'rejection_policy') and idv_1.term = 3 THEN 'PAGO_BR'
                WHEN idv_1.credit_policy_name IS null and idv_1.term > 3 THEN 'FINANCIA_BR'
                WHEN idv_1.credit_policy_name IS NOT NULL THEN 'FINANCIA_BR' ELSE 'FINANCIA_BR'
            END AS channel,
            idv_1.application_id,
            idv_1.ally_name AS ally_id,
            lower(idv_1.ally_name) AS ally,
            idv_1.client_id AS prospect_id,
            idv_1.idv_status_new AS idv_status,
            idv_1.identitywastarted_at,
            idv_1.identitywaapproved_at,
            idv_1.identitywarejected_at,
            idv_1.identitywadiscarded_at,
            idv_1.identitywadiscardedbyrisk_at,
            CASE WHEN idv_1.idv_status_new = 'approved' THEN 1 ELSE 0 END AS is_approved,
            CASE WHEN idv_1.idv_status_new = 'rejected' THEN 1 ELSE 0 END AS is_rejected,
            CASE WHEN idv_1.idv_status_new = 'discarded' THEN 1 ELSE 0 END AS is_discarded,
            CASE WHEN idv_1.idv_status_new = 'pending' THEN 1 ELSE 0 END AS is_pending,
            idv_1.requested_amount AS ticket,
            idv_1.observations,
            u.email AS analyst,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(HOUR FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(HOUR FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' AND idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(HOUR FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(MINUTE FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(MINUTE FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' AND idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(MINUTE FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(SECOND FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(SECOND FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' AND idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(SECOND FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2) AS aht_hours_rejected,
            CASE WHEN idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(HOUR FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2) END AS aht_hours_discarded,
            round(extract(MINUTE FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2) AS aht_minutes_rejected,
            CASE WHEN idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(MINUTE FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2) END AS aht_minutes_discarded,
            round(extract(SECOND FROM (idv_1.identitywaapproved_at-idv_1.identitywastarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idv_1.identitywarejected_at-idv_1.identitywastarted_at)),2) AS aht_seconds_rejected,
            CASE WHEN idv_1.identitywadiscarded_at > idv_1.identitywastarted_at THEN round(extract(SECOND FROM (idv_1.identitywadiscarded_at-idv_1.identitywastarted_at)),2) END AS aht_seconds_discarded
        FROM idv_data_step_1 idv_1 LEFT JOIN bronze.identity_management_users_co u ON u.id = idv_1.agent_user_id
        LEFT JOIN underwiting_stages udw                                           ON udw.application_id = idv_1.application_id
    )
    SELECT
        period,
        'BR' as country_code,
        'idv_2.0' as idv_version,
        application_date,
        application_id,
        idv_status,
        identitywastarted_at,
        identitywaapproved_at,
        identitywarejected_at,
        identitywadiscarded_at,
        NULL AS identityphotosevaluationstarted_at,
        NULL AS identityphotosagentassigned_at,
        analyst,
        channel,
        idv_policy,
        reason,
        ally_id,
        ally,
        prospect_id,
        ticket,
        observations,
        NULL AS photo_quality,
        count(DISTINCT application_id) AS total_apps,
        count(DISTINCT analyst) AS active_analyst,
        sum(is_approved) AS idv_approved,
        sum(is_rejected) AS idv_rejected,
        sum(is_discarded) AS idv_discarded,
        sum(is_pending) AS idv_pending,
        round(percentile(aht_hours * 3600 + aht_minutes * 60 + aht_seconds, 0.5), 2) AS aht,
        round(percentile(aht_hours_approval * 3600 + aht_minutes_approval * 60 + aht_seconds_approval, 0.5), 2) AS aht_approval,
        round(percentile(aht_hours_rejected * 3600 + aht_minutes_rejected * 60 + aht_seconds_rejected, 0.5), 2) AS aht_rejected,
        round(percentile(aht_hours_discarded * 3600 + aht_minutes_discarded * 60 + aht_seconds_discarded, 0.5), 2) AS aht_discarded,
        NULL AS aht_handling,
        NULL AS aht_queued,
        NULL AS aht_queued_addi
    FROM idv_data_step_2
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,
br_identityphotosstarted AS (
    WITH idv_data_step_1 AS (
        SELECT
            app.application_id,
            app.client_id,
            from_utc_timestamp(app.application_date, 'America/Sao_Paulo') AS application_date,
            app.client_type,
            app.journey_name,
            app.product AS product,
            idv.agent_user_id,
            idv.used_policy_id,
            idv.reason,
            idv.observations,
            from_utc_timestamp(idv.identityphotosstarted_at, 'America/Sao_Paulo') AS identityphotosstarted_at,
            from_utc_timestamp(idv.identityphotosapproved_at, 'America/Sao_Paulo') AS identityphotosapproved_at,
            from_utc_timestamp(idv.identityphotosrejected_at, 'America/Sao_Paulo') AS identityphotosrejected_at,
            from_utc_timestamp(idv.identityphotosdiscarded_at, 'America/Sao_Paulo') AS identityphotosdiscarded_at,
            from_utc_timestamp(idv.identityphotosevaluationstarted_at, 'America/Sao_Paulo') AS identityphotosevaluationstarted_at,
            from_utc_timestamp(idv.identityphotosagentassigned_at, 'America/Sao_Paulo') AS identityphotosagentassigned_at,
            from_utc_timestamp(idv.identityphotosdiscardedbyrisk_at, 'America/Sao_Paulo') AS identityphotosdiscardedbyrisk_at,
            rmt.credit_policy_name,
            rmt.term,
            rmt.approved_amount,
            rmt.requested_amount,
            rmt.ally_name,
            NULL AS photo_quality,
            CASE
                WHEN idv.identityphotosapproved_at IS NOT NULL THEN 'approved'
                WHEN idv.identityphotosrejected_at IS NOT NULL THEN 'rejected'
                WHEN (idv.identityphotosdiscarded_at IS NOT NULL or idv.identityphotosdiscardedbyrisk_at IS NOT NULL) THEN 'discarded'
                ELSE 'pending'
            END AS idv_status_new
        FROM {{ ref('f_applications_br') }} app
        LEFT JOIN {{ ref('f_idv_stage_br') }} idv                         ON app.application_id = idv.application_id
        LEFT JOIN {{ ref('risk_master_table_br') }} rmt                     ON app.application_id = rmt.application_id
        WHERE idv.identityphotosstarted_at IS NOT NULL
            AND app.client_type = 'PROSPECT'
    )
    ,
    underwiting_stages AS (
        SELECT
            DISTINCT application_id
        FROM {{ ref('f_origination_events_br_logs') }}
        WHERE journey_stage_name ilike '%underwriting-br%' OR journey_stage_name ilike '%underwriting-psychometric-br%'
    )
    ,
    idv_data_step_2 AS (
        SELECT
            to_date(idv_1.application_date) AS period,
            idv_1.application_date,
            idv_1.used_policy_id AS idv_policy,
            lower(idv_1.reason) AS reason,
            CASE
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%PAGO%' THEN 'PAGO_BR'
                WHEN idv_1.product IS NOT NULL and idv_1.product like '%FINANCIA%' THEN 'FINANCIA_BR'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%PAGO%' THEN 'PAGO_BR'
                WHEN idv_1.journey_name IS NOT NULL and idv_1.journey_name like '%FINANCIA%' THEN 'FINANCIA_BR'
                WHEN idv_1.credit_policy_name in (
                  'addipago_0aprfga_policy',
                  'addipago_0fga_policy',
                  'addipago_claro_policy',
                  'addipago_mario_h_policy',
                  'addipago_no_history_policy',
                  'addipago_policy',
                  'addipago_policy_amoblando',
                  'adelante_policy_pago',
                  'closing_policy_pago',
                  'default_policy_pago',
                  'finalization_policy_pago',
                  'rc_0aprfga',
                  'rc_0fga',
                  'rc_addipago_policy_amoblando',
                  'rc_adelante_policy',
                  'rc_closing_policy',
                  'rc_finalization_policy',
                  'rc_pago_0aprfga',
                  'rc_pago_0fga',
                  'rc_pago_claro',
                  'rc_pago_mario_h',
                  'rc_pago_standard',
                  'rc_rejection_policy',
                  'rc_standard',
                  'rejection_policy_pago') THEN 'PAGO_BR'
                WHEN idv_1.term > 3 THEN 'FINANCIA_BR'
                WHEN idv_1.credit_policy_name in ('closing_policy','finalization_policy', 'rejection_policy') and idv_1.term = 3 THEN 'PAGO_BR'
                WHEN idv_1.credit_policy_name IS null and idv_1.term > 3 THEN 'FINANCIA_BR'
                WHEN idv_1.credit_policy_name IS NOT NULL THEN 'FINANCIA_BR' ELSE 'FINANCIA_BR'
            END AS channel,
            idv_1.application_id,
            idv_1.ally_name AS ally_id,
            lower(idv_1.ally_name) AS ally,
            idv_1.client_id AS prospect_id,
            idv_1.idv_status_new AS idv_status,
            idv_1.identityphotosstarted_at AS identitywastarted_at,
            idv_1.identityphotosapproved_at AS identitywaapproved_at,
            idv_1.identityphotosrejected_at AS identitywarejected_at,
            idv_1.identityphotosdiscarded_at AS identitywadiscarded_at,
            idv_1.identityphotosevaluationstarted_at,
            idv_1.identityphotosagentassigned_at,
            CASE WHEN idv_1.idv_status_new = 'approved' THEN 1 ELSE 0 END AS is_approved,
            CASE WHEN idv_1.idv_status_new = 'rejected' THEN 1 ELSE 0 END AS is_rejected,
            CASE WHEN idv_1.idv_status_new = 'discarded' THEN 1 ELSE 0 END AS is_discarded,
            CASE WHEN idv_1.idv_status_new = 'pending' THEN 1 ELSE 0 END AS is_pending,
            idv_1.requested_amount AS ticket,
            idv_1.observations,
            u.email AS analyst,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(HOUR FROM (idv_1.identityphotosapproved_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(HOUR FROM (idv_1.identityphotosrejected_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' THEN round(extract(HOUR FROM (idv_1.identityphotosdiscarded_at - idv_1.identityphotosstarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(MINUTE FROM (idv_1.identityphotosapproved_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(MINUTE FROM (idv_1.identityphotosrejected_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' THEN round(extract(MINUTE FROM (idv_1.identityphotosdiscarded_at - idv_1.identityphotosstarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idv_1.idv_status_new = 'approved' THEN round(extract(SECOND FROM (idv_1.identityphotosapproved_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'rejected' THEN round(extract(SECOND FROM (idv_1.identityphotosrejected_at - idv_1.identityphotosstarted_at)),2)
                WHEN idv_1.idv_status_new = 'discarded' THEN round(extract(SECOND FROM (idv_1.identityphotosdiscarded_at - idv_1.identityphotosstarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idv_1.identityphotosapproved_at-idv_1.identityphotosstarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idv_1.identityphotosrejected_at-idv_1.identityphotosstarted_at)),2) AS aht_hours_rejected,
            round(extract(HOUR FROM (idv_1.identityphotosdiscarded_at-idv_1.identityphotosstarted_at)),2) AS aht_hours_discarded,
            round(extract(HOUR FROM (coalesce(idv_1.identityphotosapproved_at,idv_1.identityphotosrejected_at,idv_1.identityphotosdiscarded_at,idv_1.identityphotosdiscardedbyrisk_at) - idv_1.identityphotosagentassigned_at)),2) AS aht_hours_handling,
            round(extract(HOUR FROM (idv_1.identityphotosevaluationstarted_at - idv_1.identityphotosstarted_at)),2) AS aht_hours_queued,
            round(extract(HOUR FROM (idv_1.identityphotosagentassigned_at - idv_1.identityphotosevaluationstarted_at)),2) AS aht_hours_queued_addi,
            round(extract(MINUTE FROM (idv_1.identityphotosapproved_at-idv_1.identityphotosstarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idv_1.identityphotosrejected_at-idv_1.identityphotosstarted_at)),2) AS aht_minutes_rejected,
            round(extract(MINUTE FROM (idv_1.identityphotosdiscarded_at-idv_1.identityphotosstarted_at)),2) AS aht_minutes_discarded,
            round(extract(MINUTE FROM (coalesce(idv_1.identityphotosapproved_at,idv_1.identityphotosrejected_at,idv_1.identityphotosdiscarded_at,idv_1.identityphotosdiscardedbyrisk_at) - idv_1.identityphotosagentassigned_at)),2) AS aht_minutes_handling,
            round(extract(MINUTE FROM (idv_1.identityphotosevaluationstarted_at - idv_1.identityphotosstarted_at)),2) AS aht_minutes_queued,
            round(extract(MINUTE FROM (idv_1.identityphotosagentassigned_at - idv_1.identityphotosevaluationstarted_at)),2) AS aht_minutes_queued_addi,
            round(extract(SECOND FROM (idv_1.identityphotosapproved_at-idv_1.identityphotosstarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idv_1.identityphotosrejected_at-idv_1.identityphotosstarted_at)),2) AS aht_seconds_rejected,
            round(extract(SECOND FROM (idv_1.identityphotosdiscarded_at-idv_1.identityphotosstarted_at)),2) AS aht_seconds_discarded,
            round(extract(SECOND FROM (coalesce(idv_1.identityphotosapproved_at,idv_1.identityphotosrejected_at,idv_1.identityphotosdiscarded_at,idv_1.identityphotosdiscardedbyrisk_at) - idv_1.identityphotosagentassigned_at)),2) AS aht_seconds_handling,
            round(extract(SECOND FROM (idv_1.identityphotosevaluationstarted_at - idv_1.identityphotosstarted_at)),2) AS aht_seconds_queued,
            round(extract(SECOND FROM (idv_1.identityphotosagentassigned_at - idv_1.identityphotosevaluationstarted_at)),2) AS aht_seconds_queued_addi,
            idv_1.photo_quality
        FROM idv_data_step_1 idv_1 LEFT JOIN bronze.identity_management_users_co u ON u.id = idv_1.agent_user_id
        LEFT JOIN underwiting_stages udw                                           ON udw.application_id = idv_1.application_id
    )
    SELECT
        period,
        'CO' as country_code,
        'idv_4.0' as idv_version,
        application_date,
        application_id,
        idv_status,
        identitywastarted_at,
        identitywaapproved_at,
        identitywarejected_at,
        identitywadiscarded_at,
        identityphotosevaluationstarted_at,
        identityphotosagentassigned_at,
        analyst,
        channel,
        idv_policy,
        reason,
        ally_id,
        ally,
        prospect_id,
        ticket,
        observations,
        photo_quality,
        count(DISTINCT application_id) AS total_apps,
        count(DISTINCT analyst) AS active_analyst,
        sum(is_approved) AS idv_approved,
        sum(is_rejected) AS idv_rejected,
        sum(is_discarded) AS idv_discarded,
        sum(is_pending) AS idv_pending,
        round(percentile(aht_hours * 3600 + aht_minutes * 60 + aht_seconds, 0.5), 2) AS aht,
        round(percentile(aht_hours_approval * 3600 + aht_minutes_approval * 60 + aht_seconds_approval, 0.5), 2) AS aht_approval,
        round(percentile(aht_hours_rejected * 3600 + aht_minutes_rejected * 60 + aht_seconds_rejected, 0.5), 2) AS aht_rejected,
        round(percentile(aht_hours_discarded * 3600 + aht_minutes_discarded * 60 + aht_seconds_discarded, 0.5), 2) AS aht_discarded,
        round(percentile(aht_hours_handling * 3600 + aht_minutes_handling * 60 + aht_seconds_handling, 0.5), 2) AS aht_handling,
        round(percentile(aht_hours_queued * 3600 + aht_minutes_queued * 60 + aht_seconds_queued, 0.5), 2) AS aht_queued,
        round(percentile(aht_hours_queued_addi * 3600 + aht_minutes_queued_addi * 60 + aht_seconds_queued_addi, 0.5), 2) AS aht_queued_addi
    FROM idv_data_step_2
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
SELECT * FROM co_identitywastarted
UNION ALL
SELECT * FROM co_identityphotosstarted
UNION ALL
SELECT * FROM br_identitywastarted
UNION ALL
SELECT * FROM br_identityphotosstarted