WITH 

-- IDV STAGE TABLES

f_idv_stage_co AS (

    SELECT
        *,
        CASE
            WHEN identitywaapproved_at IS NOT NULL THEN 'approved'
            WHEN identitywarejected_at IS NOT NULL THEN 'rejected'
            WHEN (identitywadiscarded_at IS NOT NULL OR identitywadiscardedbyrisk_at IS NOT NULL)
                AND (identitywadiscarded_at > identitywastarted_at OR identitywadiscardedbyrisk_at > identitywastarted_at)
                THEN 'discarded'
            ELSE 'pending'
        END AS idv_status_v2,
        CASE
            WHEN identitywaapproved_at IS NOT NULL THEN 'approved'
            WHEN identitywarejected_at IS NOT NULL THEN 'rejected'
            WHEN (identitywadiscarded_at IS NOT NULL OR identitywadiscardedbyrisk_at IS NOT NULL)
                AND (identitywadiscarded_at > identitywastarted_at OR identitywadiscardedbyrisk_at > identitywastarted_at)
                THEN 'discarded'
            ELSE 'pending'
        END AS idv_status_v4
    FROM silver.f_idv_stage_co

),

f_idv_stage_br AS (

    SELECT
        *,
        CASE
            WHEN identitywaapproved_at IS NOT NULL THEN 'approved'
            WHEN identitywarejected_at IS NOT NULL THEN 'rejected'
            WHEN (identitywadiscarded_at IS NOT NULL OR identitywadiscardedbyrisk_at IS NOT NULL)
                AND (identitywadiscarded_at > identitywastarted_at OR identitywadiscardedbyrisk_at > identitywastarted_at)
                THEN 'discarded'
            ELSE 'pending'
        END AS idv_status_v2,
        CASE
            WHEN identitywaapproved_at IS NOT NULL THEN 'approved'
            WHEN identitywarejected_at IS NOT NULL THEN 'rejected'
            WHEN (identitywadiscarded_at IS NOT NULL OR identitywadiscardedbyrisk_at IS NOT NULL)
                AND (identitywadiscarded_at > identitywastarted_at OR identitywadiscardedbyrisk_at > identitywastarted_at)
                THEN 'discarded'
            ELSE 'pending'
        END AS idv_status_v4
    FROM silver.f_idv_stage_br

),

-- APPLICATIONS TABLES

f_applications_co AS (

    SELECT
        *
    FROM silver.f_applications_co

),

f_applications_br AS (

    SELECT
        *
    FROM silver.f_applications_br

),

-- IDENTITY MANAGEMENT USERS TABLES


d_identity_management_users_co AS (

    SELECT
        *
    FROM silver.d_identity_management_users_co

),

d_identity_management_users_br AS (

    SELECT
        *
    FROM silver.d_identity_management_users_br

),

-- IDV V2


query_idv_2_co AS (

    SELECT

        raw.period,
        raw.country_code,
        'idv_2.0' AS idv_version,
        raw.application_date,
        raw.application_id,
        raw.idv_status,
        raw.identitywastarted_at,
        raw.identitywaapproved_at,
        raw.identitywarejected_at,
        raw.identitywadiscarded_at,
        CAST(NULL AS string) AS identityphotosevaluationstarted_at,
        CAST(NULL AS string) AS identityphotosagentassigned_at,
        raw.analyst,
        raw.channel,
        raw.idv_policy,
        raw.reason,
        raw.ally_id,
        raw.ally,
        raw.prospect_id,
        raw.ticket,
        raw.observations,
        count(DISTINCT raw.application_id) AS total_apps,
        count(DISTINCT raw.analyst) AS active_analyst,
        sum(raw.is_approved) AS idv_approved,
        sum(raw.is_rejected) AS idv_rejected,
        sum(raw.is_discarded) AS idv_discarded,
        sum(raw.is_pending) AS idv_pending,
        round(percentile(aht_hours*3600+aht_minutes*60+aht_seconds,0.5),2) AS aht,
        round(percentile(aht_hours_approval*3600+aht_minutes_approval*60+aht_seconds_approval,0.5),2) AS aht_approval,
        round(percentile(aht_hours_rejected*3600+aht_minutes_rejected*60+aht_seconds_rejected,0.5),2) AS aht_rejected,
        round(percentile(aht_hours_discarded*3600+aht_minutes_discarded*60+aht_seconds_discarded,0.5),2) AS aht_discarded,
        null AS aht_handling,
        null AS aht_queued,
        null AS aht_queued_addi,
        md5(cast(concat(coalesce(cast(raw.country_code as 
    string
), ''), '-', coalesce(cast(raw.application_id as 
    string
), '')) as 
    string
)) AS surrogate_key

    FROM (

        SELECT
            "CO" AS country_code,
            to_date(from_utc_timestamp(idvs.identitywastarted_at, "America/Bogota" )) AS period,
            from_utc_timestamp(apps.application_date, "America/Bogota" ) AS application_date,
            idvs.used_policy_id AS idv_policy,
            lower(idvs.reason) AS reason,
            CASE WHEN apps.custom_is_santander_branched IS NOT NULL THEN "SANTANDER_CO" ELSE apps.product END AS channel,
            idvs.application_id,
            apps.ally_slug AS ally_id,
            apps.ally_slug AS ally,
            apps.client_id AS prospect_id,
            idvs.idv_status_v2 AS idv_status,
            from_utc_timestamp(idvs.identitywastarted_at, "America/Bogota" ) AS identitywastarted_at,
            from_utc_timestamp(idvs.identitywaapproved_at, "America/Bogota" ) AS identitywaapproved_at,
            from_utc_timestamp(idvs.identitywarejected_at, "America/Bogota" ) AS identitywarejected_at,
            from_utc_timestamp(idvs.identitywadiscarded_at, "America/Bogota" ) AS identitywadiscarded_at,
            CASE WHEN idvs.idv_status_v2 = 'approved' THEN 1 else 0 END AS is_approved,
            CASE WHEN idvs.idv_status_v2 = 'rejected' THEN 1 else 0 END AS is_rejected,
            CASE WHEN idvs.idv_status_v2 = 'discarded' THEN 1 else 0 END AS is_discarded,
            CASE WHEN idvs.idv_status_v2 = 'pending' THEN 1 else 0 END AS is_pending,
            apps.requested_amount AS ticket,
            idvs.observations AS observations,
            imus.email AS analyst,
            CASE
                WHEN idvs.idv_status_v2 = 'approved' THEN round(extract(HOUR FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'rejected' THEN round(extract(HOUR FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'discarded' THEN round(extract(HOUR FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idvs.idv_status_v2 = 'approved' THEN round(extract(MINUTE FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'rejected' THEN round(extract(MINUTE FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'discarded' THEN round(extract(MINUTE FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idvs.idv_status_v2 = 'approved' THEN round(extract(SECOND FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'rejected' THEN round(extract(SECOND FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'discarded' THEN round(extract(SECOND FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2) AS aht_hours_rejected,
            CASE WHEN idvs.identitywadiscarded_at > idvs.identitywastarted_at THEN round(extract(HOUR FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2) END AS aht_hours_discarded,
            round(extract(MINUTE FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2) AS aht_minutes_rejected,
            CASE WHEN idvs.identitywadiscarded_at > idvs.identitywastarted_at THEN round(extract(MINUTE FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2) END AS aht_minutes_discarded,
            round(extract(SECOND FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2) AS aht_seconds_rejected,
            CASE WHEN idvs.identitywadiscarded_at > idvs.identitywastarted_at THEN round(extract(SECOND FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2) END AS aht_seconds_discarded

        FROM (

            SELECT
                *
            FROM f_idv_stage_co
            WHERE identitywastarted_at IS NOT NULL) AS idvs

        LEFT JOIN f_applications_co AS apps
            ON idvs.application_id = apps.application_id
        LEFT JOIN d_identity_management_users_co imus
            ON idvs.agent_user_id = imus.id
        WHERE apps.client_type = 'PROSPECT'
        ) raw
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

),

query_idv_2_br AS (

    SELECT

        raw.period,
        raw.country_code,
        'idv_2.0' AS idv_version,
        raw.application_date,
        raw.application_id,
        raw.idv_status,
        raw.identitywastarted_at,
        raw.identitywaapproved_at,
        raw.identitywarejected_at,
        raw.identitywadiscarded_at,
        CAST(NULL AS string) AS identityphotosevaluationstarted_at,
        CAST(NULL AS string) AS identityphotosagentassigned_at,
        raw.analyst,
        raw.channel,
        raw.idv_policy,
        raw.reason,
        raw.ally_id,
        raw.ally,
        raw.prospect_id,
        raw.ticket,
        raw.observations,
        count(DISTINCT raw.application_id) AS total_apps,
        count(DISTINCT raw.analyst) AS active_analyst,
        sum(raw.is_approved) AS idv_approved,
        sum(raw.is_rejected) AS idv_rejected,
        sum(raw.is_discarded) AS idv_discarded,
        sum(raw.is_pending) AS idv_pending,
        round(percentile(aht_hours*3600+aht_minutes*60+aht_seconds,0.5),2) AS aht,
        round(percentile(aht_hours_approval*3600+aht_minutes_approval*60+aht_seconds_approval,0.5),2) AS aht_approval,
        round(percentile(aht_hours_rejected*3600+aht_minutes_rejected*60+aht_seconds_rejected,0.5),2) AS aht_rejected,
        round(percentile(aht_hours_discarded*3600+aht_minutes_discarded*60+aht_seconds_discarded,0.5),2) AS aht_discarded,
        null AS aht_handling,
        null AS aht_queued,
        null AS aht_queued_addi,
        md5(cast(concat(coalesce(cast(raw.country_code as 
    string
), ''), '-', coalesce(cast(raw.application_id as 
    string
), '')) as 
    string
)) AS surrogate_key

    FROM (

        SELECT
            "BR" AS country_code,
            to_date(from_utc_timestamp(idvs.identitywastarted_at, "America/Sao_Paulo" )) AS period,
            from_utc_timestamp(apps.application_date, "America/Sao_Paulo" ) AS application_date,
            idvs.used_policy_id AS idv_policy,
            lower(idvs.reason) AS reason,
            apps.product AS channel,
            idvs.application_id,
            apps.ally_slug AS ally_id,
            apps.ally_slug AS ally,
            apps.client_id AS prospect_id,
            idvs.idv_status_v2 AS idv_status,
            from_utc_timestamp(idvs.identitywastarted_at, "America/Sao_Paulo" ) AS identitywastarted_at,
            from_utc_timestamp(idvs.identitywaapproved_at, "America/Sao_Paulo" ) AS identitywaapproved_at,
            from_utc_timestamp(idvs.identitywarejected_at, "America/Sao_Paulo" ) AS identitywarejected_at,
            from_utc_timestamp(idvs.identitywadiscarded_at, "America/Sao_Paulo" ) AS identitywadiscarded_at,
            CASE WHEN idvs.idv_status_v2 = 'approved' THEN 1 else 0 END AS is_approved,
            CASE WHEN idvs.idv_status_v2 = 'rejected' THEN 1 else 0 END AS is_rejected,
            CASE WHEN idvs.idv_status_v2 = 'discarded' THEN 1 else 0 END AS is_discarded,
            CASE WHEN idvs.idv_status_v2 = 'pending' THEN 1 else 0 END AS is_pending,
            apps.requested_amount AS ticket,
            idvs.observations AS observations,
            imus.email AS analyst,
            CASE
                WHEN idvs.idv_status_v2 = 'approved' THEN round(extract(HOUR FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'rejected' THEN round(extract(HOUR FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'discarded' THEN round(extract(HOUR FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idvs.idv_status_v2 = 'approved' THEN round(extract(MINUTE FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'rejected' THEN round(extract(MINUTE FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'discarded' THEN round(extract(MINUTE FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idvs.idv_status_v2 = 'approved' THEN round(extract(SECOND FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'rejected' THEN round(extract(SECOND FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
                WHEN idvs.idv_status_v2 = 'discarded' THEN round(extract(SECOND FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2) AS aht_hours_rejected,
            CASE WHEN idvs.identitywadiscarded_at > idvs.identitywastarted_at THEN round(extract(HOUR FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2) END AS aht_hours_discarded,
            round(extract(MINUTE FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2) AS aht_minutes_rejected,
            CASE WHEN idvs.identitywadiscarded_at > idvs.identitywastarted_at THEN round(extract(MINUTE FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2) END AS aht_minutes_discarded,
            round(extract(SECOND FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2) AS aht_seconds_rejected,
            CASE WHEN idvs.identitywadiscarded_at > idvs.identitywastarted_at THEN round(extract(SECOND FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2) END AS aht_seconds_discarded

        FROM (

            SELECT
                *
            FROM f_idv_stage_br
            WHERE identitywastarted_at IS NOT NULL) AS idvs

        LEFT JOIN f_applications_br AS apps
            ON idvs.application_id = apps.application_id
        LEFT JOIN d_identity_management_users_br imus
            ON idvs.agent_user_id = imus.id
        WHERE apps.client_type = 'PROSPECT'
        ) raw
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

),

-- IDV V4

query_idv_4_co AS (

    SELECT

        raw.period,
        raw.country_code,
        'idv_4.0' as idv_version,
        raw.application_date,
        raw.application_id,
        raw.idv_status,
        raw.identitywastarted_at,
        raw.identitywaapproved_at,
        raw.identitywarejected_at,
        raw.identitywadiscarded_at,
        raw.identityphotosevaluationstarted_at,
        raw.identityphotosagentassigned_at,
        raw.analyst,
        raw.channel,
        raw.idv_policy,
        raw.reason,
        raw.ally_id,
        raw.ally,
        raw.prospect_id,
        raw.ticket,
        raw.observations,
        count(DISTINCT raw.application_id) AS total_apps,
        count(DISTINCT raw.analyst) AS active_analyst,
        sum(raw.is_approved) AS idv_approved,
        sum(raw.is_rejected) AS idv_rejected,
        sum(raw.is_discarded) AS idv_discarded,
        sum(raw.is_pending) AS idv_pending,
        round(percentile(raw.aht_hours*3600 + raw.aht_minutes*60 + raw.aht_seconds,0.5),2) AS aht,
        round(percentile(raw.aht_hours_approval*3600 + raw.aht_minutes_approval*60 + raw.aht_seconds_approval,0.5),2) AS aht_approval,
        round(percentile(raw.aht_hours_rejected*3600 + raw.aht_minutes_rejected*60 + raw.aht_seconds_rejected,0.5),2) AS aht_rejected,
        round(percentile(raw.aht_hours_discarded*3600 + raw.aht_minutes_discarded*60 + raw.aht_seconds_discarded,0.5),2) AS aht_discarded,
        round(percentile(raw.aht_hours_handling*3600 + raw.aht_minutes_handling*60 + raw.aht_seconds_handling,0.5),2) AS aht_handling,
        round(percentile(raw.aht_hours_queued*3600 + raw.aht_minutes_queued*60 + raw.aht_seconds_queued,0.5),2) AS aht_queued,
        round(percentile(raw.aht_hours_queued_addi*3600 + raw.aht_minutes_queued_addi*60 + raw.aht_seconds_queued_addi,0.5),2) AS aht_queued_addi,
        md5(cast(concat(coalesce(cast(raw.country_code as 
    string
), ''), '-', coalesce(cast(raw.application_id as 
    string
), '')) as 
    string
)) AS surrogate_key

    FROM (

        SELECT
            "CO" AS country_code,
            to_date(from_utc_timestamp(idvs.identityphotosstarted_at, "America/Bogota" )) AS period,
            from_utc_timestamp(apps.application_date, "America/Bogota" ) AS application_date,
            idvs.used_policy_id AS idv_policy,
            lower(idvs.reason) AS reason,
            CASE WHEN apps.custom_is_santander_branched IS NOT NULL THEN "SANTANDER_CO" ELSE apps.product END AS channel,
            idvs.application_id,
            apps.ally_slug AS ally_id,
            apps.ally_slug AS ally,
            apps.client_id AS prospect_id,
            idvs.idv_status_v4 AS idv_status,
            from_utc_timestamp(idvs.identityphotosstarted_at, "America/Bogota" ) AS identitywastarted_at,
            from_utc_timestamp(idvs.identityphotosapproved_at, "America/Bogota" ) AS identitywaapproved_at,
            from_utc_timestamp(idvs.identityphotosrejected_at, "America/Bogota" ) AS identitywarejected_at,
            from_utc_timestamp(idvs.identityphotosdiscarded_at, "America/Bogota" ) AS identitywadiscarded_at,
            from_utc_timestamp(idvs.identityphotosevaluationstarted_at, "America/Bogota" ) AS identityphotosevaluationstarted_at,
            from_utc_timestamp(idvs.identityphotosagentassigned_at, "America/Bogota" ) AS identityphotosagentassigned_at,
            CASE WHEN idvs.idv_status_v4 = 'approved' THEN 1 else 0 END AS is_approved,
            CASE WHEN idvs.idv_status_v4 = 'rejected' THEN 1 else 0 END AS is_rejected,
            CASE WHEN idvs.idv_status_v4 = 'discarded' THEN 1 else 0 END AS is_discarded,
            CASE WHEN idvs.idv_status_v4 = 'pending' THEN 1 else 0 END AS is_pending,
            apps.requested_amount AS ticket,
            idvs.observations AS observations,
            imus.email AS analyst,
            CASE
                WHEN idvs.idv_status_v4 = 'approved' THEN round(extract(HOUR FROM (idvs.identityphotosapproved_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'rejected' THEN round(extract(HOUR FROM (idvs.identityphotosrejected_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'discarded' THEN round(extract(HOUR FROM (idvs.identityphotosdiscarded_at-idvs.identityphotosstarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idvs.idv_status_v4 = 'approved' THEN round(extract(MINUTE FROM (idvs.identityphotosapproved_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'rejected' THEN round(extract(MINUTE FROM (idvs.identityphotosrejected_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'discarded' THEN round(extract(MINUTE FROM (idvs.identityphotosdiscarded_at-idvs.identityphotosstarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idvs.idv_status_v4 = 'approved' THEN round(extract(SECOND FROM (idvs.identityphotosapproved_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'rejected' THEN round(extract(SECOND FROM (idvs.identityphotosrejected_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'discarded' THEN round(extract(SECOND FROM (idvs.identityphotosdiscarded_at-idvs.identityphotosstarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idvs.identityphotosapproved_at - idvs.identityphotosstarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idvs.identityphotosrejected_at - idvs.identityphotosstarted_at)),2) AS aht_hours_rejected,
            round(extract(HOUR FROM (idvs.identityphotosdiscarded_at - idvs.identityphotosstarted_at)),2) AS aht_hours_discarded,
            round(extract(HOUR FROM (coalesce(idvs.identityphotosapproved_at, idvs.identityphotosrejected_at, idvs.identityphotosdiscarded_at, idvs.identityphotosdiscardedbyrisk_at) - idvs.identityphotosagentassigned_at)),2) AS aht_hours_handling,
            round(extract(HOUR FROM (idvs.identityphotosevaluationstarted_at - idvs.identityphotosstarted_at)),2) AS aht_hours_queued,
            round(extract(HOUR FROM (idvs.identityphotosagentassigned_at - idvs.identityphotosevaluationstarted_at)),2) AS aht_hours_queued_addi,
            round(extract(MINUTE FROM (idvs.identityphotosapproved_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idvs.identityphotosrejected_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_rejected,
            round(extract(MINUTE FROM (idvs.identityphotosdiscarded_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_discarded,
            round(extract(MINUTE FROM (coalesce(idvs.identityphotosapproved_at, idvs.identityphotosrejected_at, idvs.identityphotosdiscarded_at, idvs.identityphotosdiscardedbyrisk_at) - idvs.identityphotosagentassigned_at)),2) AS aht_minutes_handling,
            round(extract(MINUTE FROM (idvs.identityphotosevaluationstarted_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_queued,
            round(extract(MINUTE FROM (idvs.identityphotosagentassigned_at - idvs.identityphotosevaluationstarted_at)),2) AS aht_minutes_queued_addi,
            round(extract(SECOND FROM (idvs.identityphotosapproved_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idvs.identityphotosrejected_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_rejected,
            round(extract(SECOND FROM (idvs.identityphotosdiscarded_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_discarded,
            round(extract(SECOND FROM (coalesce(idvs.identityphotosapproved_at, idvs.identityphotosrejected_at, idvs.identityphotosdiscarded_at, idvs.identityphotosdiscardedbyrisk_at) - idvs.identityphotosagentassigned_at)),2) AS aht_seconds_handling,
            round(extract(SECOND FROM (idvs.identityphotosevaluationstarted_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_queued,
            round(extract(SECOND FROM (idvs.identityphotosagentassigned_at - idvs.identityphotosevaluationstarted_at)),2) AS aht_seconds_queued_addi

        FROM (

            SELECT
                *
            FROM f_idv_stage_co
            WHERE identityphotosstarted_at IS NOT NULL) AS idvs

        LEFT JOIN f_applications_co AS apps
            ON idvs.application_id = apps.application_id
        LEFT JOIN d_identity_management_users_co imus
            ON idvs.agent_user_id = imus.id
        WHERE apps.client_type = 'PROSPECT'

        ) raw
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

),

query_idv_4_br AS (

    SELECT

        raw.period,
        raw.country_code,
        'idv_4.0' as idv_version,
        raw.application_date,
        raw.application_id,
        raw.idv_status,
        raw.identitywastarted_at,
        raw.identitywaapproved_at,
        raw.identitywarejected_at,
        raw.identitywadiscarded_at,
        raw.identityphotosevaluationstarted_at,
        raw.identityphotosagentassigned_at,
        raw.analyst,
        raw.channel,
        raw.idv_policy,
        raw.reason,
        raw.ally_id,
        raw.ally,
        raw.prospect_id,
        raw.ticket,
        raw.observations,
        count(DISTINCT raw.application_id) AS total_apps,
        count(DISTINCT raw.analyst) AS active_analyst,
        sum(raw.is_approved) AS idv_approved,
        sum(raw.is_rejected) AS idv_rejected,
        sum(raw.is_discarded) AS idv_discarded,
        sum(raw.is_pending) AS idv_pending,
        round(percentile(raw.aht_hours*3600 + raw.aht_minutes*60 + raw.aht_seconds,0.5),2) AS aht,
        round(percentile(raw.aht_hours_approval*3600 + raw.aht_minutes_approval*60 + raw.aht_seconds_approval,0.5),2) AS aht_approval,
        round(percentile(raw.aht_hours_rejected*3600 + raw.aht_minutes_rejected*60 + raw.aht_seconds_rejected,0.5),2) AS aht_rejected,
        round(percentile(raw.aht_hours_discarded*3600 + raw.aht_minutes_discarded*60 + raw.aht_seconds_discarded,0.5),2) AS aht_discarded,
        round(percentile(raw.aht_hours_handling*3600 + raw.aht_minutes_handling*60 + raw.aht_seconds_handling,0.5),2) AS aht_handling,
        round(percentile(raw.aht_hours_queued*3600 + raw.aht_minutes_queued*60 + raw.aht_seconds_queued,0.5),2) AS aht_queued,
        round(percentile(raw.aht_hours_queued_addi*3600 + raw.aht_minutes_queued_addi*60 + raw.aht_seconds_queued_addi,0.5),2) AS aht_queued_addi,
        md5(cast(concat(coalesce(cast(raw.country_code as 
    string
), ''), '-', coalesce(cast(raw.application_id as 
    string
), '')) as 
    string
)) AS surrogate_key

    FROM (

        SELECT
            "BR" AS country_code,
            to_date(from_utc_timestamp(idvs.identityphotosstarted_at, "America/Sao_Paulo" )) AS period,
            from_utc_timestamp(apps.application_date, "America/Sao_Paulo" ) AS application_date,
            idvs.used_policy_id AS idv_policy,
            lower(idvs.reason) AS reason,
            apps.product AS channel,
            idvs.application_id,
            apps.ally_slug AS ally_id,
            apps.ally_slug AS ally,
            apps.client_id AS prospect_id,
            idvs.idv_status_v4 AS idv_status,
            from_utc_timestamp(idvs.identityphotosstarted_at, "America/Sao_Paulo" ) AS identitywastarted_at,
            from_utc_timestamp(idvs.identityphotosapproved_at, "America/Sao_Paulo" ) AS identitywaapproved_at,
            from_utc_timestamp(idvs.identityphotosrejected_at, "America/Sao_Paulo" ) AS identitywarejected_at,
            from_utc_timestamp(idvs.identityphotosdiscarded_at, "America/Sao_Paulo" ) AS identitywadiscarded_at,
            from_utc_timestamp(idvs.identityphotosevaluationstarted_at, "America/Sao_Paulo" ) AS identityphotosevaluationstarted_at,
            from_utc_timestamp(idvs.identityphotosagentassigned_at, "America/Sao_Paulo" ) AS identityphotosagentassigned_at,
            CASE WHEN idvs.idv_status_v4 = 'approved' THEN 1 else 0 END AS is_approved,
            CASE WHEN idvs.idv_status_v4 = 'rejected' THEN 1 else 0 END AS is_rejected,
            CASE WHEN idvs.idv_status_v4 = 'discarded' THEN 1 else 0 END AS is_discarded,
            CASE WHEN idvs.idv_status_v4 = 'pending' THEN 1 else 0 END AS is_pending,
            apps.requested_amount AS ticket,
            idvs.observations AS observations,
            imus.email AS analyst,
            CASE
                WHEN idvs.idv_status_v4 = 'approved' THEN round(extract(HOUR FROM (idvs.identityphotosapproved_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'rejected' THEN round(extract(HOUR FROM (idvs.identityphotosrejected_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'discarded' THEN round(extract(HOUR FROM (idvs.identityphotosdiscarded_at-idvs.identityphotosstarted_at)),2)
            END AS aht_hours,
            CASE
                WHEN idvs.idv_status_v4 = 'approved' THEN round(extract(MINUTE FROM (idvs.identityphotosapproved_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'rejected' THEN round(extract(MINUTE FROM (idvs.identityphotosrejected_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'discarded' THEN round(extract(MINUTE FROM (idvs.identityphotosdiscarded_at-idvs.identityphotosstarted_at)),2)
            END AS aht_minutes,
            CASE
                WHEN idvs.idv_status_v4 = 'approved' THEN round(extract(SECOND FROM (idvs.identityphotosapproved_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'rejected' THEN round(extract(SECOND FROM (idvs.identityphotosrejected_at-idvs.identityphotosstarted_at)),2)
                WHEN idvs.idv_status_v4 = 'discarded' THEN round(extract(SECOND FROM (idvs.identityphotosdiscarded_at-idvs.identityphotosstarted_at)),2)
            END AS aht_seconds,
            round(extract(HOUR FROM (idvs.identityphotosapproved_at - idvs.identityphotosstarted_at)),2) AS aht_hours_approval,
            round(extract(HOUR FROM (idvs.identityphotosrejected_at - idvs.identityphotosstarted_at)),2) AS aht_hours_rejected,
            round(extract(HOUR FROM (idvs.identityphotosdiscarded_at - idvs.identityphotosstarted_at)),2) AS aht_hours_discarded,
            round(extract(HOUR FROM (coalesce(idvs.identityphotosapproved_at, idvs.identityphotosrejected_at, idvs.identityphotosdiscarded_at, idvs.identityphotosdiscardedbyrisk_at) - idvs.identityphotosagentassigned_at)),2) AS aht_hours_handling,
            round(extract(HOUR FROM (idvs.identityphotosevaluationstarted_at - idvs.identityphotosstarted_at)),2) AS aht_hours_queued,
            round(extract(HOUR FROM (idvs.identityphotosagentassigned_at - idvs.identityphotosevaluationstarted_at)),2) AS aht_hours_queued_addi,
            round(extract(MINUTE FROM (idvs.identityphotosapproved_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_approval,
            round(extract(MINUTE FROM (idvs.identityphotosrejected_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_rejected,
            round(extract(MINUTE FROM (idvs.identityphotosdiscarded_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_discarded,
            round(extract(MINUTE FROM (coalesce(idvs.identityphotosapproved_at, idvs.identityphotosrejected_at, idvs.identityphotosdiscarded_at, idvs.identityphotosdiscardedbyrisk_at) - idvs.identityphotosagentassigned_at)),2) AS aht_minutes_handling,
            round(extract(MINUTE FROM (idvs.identityphotosevaluationstarted_at - idvs.identityphotosstarted_at)),2) AS aht_minutes_queued,
            round(extract(MINUTE FROM (idvs.identityphotosagentassigned_at - idvs.identityphotosevaluationstarted_at)),2) AS aht_minutes_queued_addi,
            round(extract(SECOND FROM (idvs.identityphotosapproved_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_approval,
            round(extract(SECOND FROM (idvs.identityphotosrejected_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_rejected,
            round(extract(SECOND FROM (idvs.identityphotosdiscarded_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_discarded,
            round(extract(SECOND FROM (coalesce(idvs.identityphotosapproved_at, idvs.identityphotosrejected_at, idvs.identityphotosdiscarded_at, idvs.identityphotosdiscardedbyrisk_at) - idvs.identityphotosagentassigned_at)),2) AS aht_seconds_handling,
            round(extract(SECOND FROM (idvs.identityphotosevaluationstarted_at - idvs.identityphotosstarted_at)),2) AS aht_seconds_queued,
            round(extract(SECOND FROM (idvs.identityphotosagentassigned_at - idvs.identityphotosevaluationstarted_at)),2) AS aht_seconds_queued_addi

        FROM (

            SELECT
                *
            FROM f_idv_stage_br
            WHERE identityphotosstarted_at IS NOT NULL) AS idvs

        LEFT JOIN f_applications_br AS apps
            ON idvs.application_id = apps.application_id
        LEFT JOIN d_identity_management_users_br imus
            ON idvs.agent_user_id = imus.id
        WHERE apps.client_type = 'PROSPECT'

        ) raw
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

)

SELECT * FROM query_idv_2_co

UNION ALL

SELECT * FROM query_idv_4_co

UNION ALL

SELECT * FROM query_idv_2_br

UNION ALL

SELECT * FROM query_idv_4_br



