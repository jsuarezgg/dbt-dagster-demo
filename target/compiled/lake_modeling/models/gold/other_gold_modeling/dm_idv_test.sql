

SELECT

    raw.period,
    raw.country_code,
    'idv_2.0' AS idv_version,
    raw.application_date,
    raw.application_id,
    raw.idv_status,
    raw.identitywastarted_at,
    raw.identitywaapproved_at,
    raw.identitywaapproved_at,
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
        to_date(coalesce(idvs.identitywastarted_at, idvs.prospectidentityverificationstarted_at)) AS period,
        apps.application_date AS application_date,
        idvs.used_policy_id AS idv_policy,
        lower(idvs.reason) AS reason,
        CASE
            WHEN apps.custom_is_santander_branched IS NOT NULL THEN 'SANTANDER_CO'
            ELSE apps.product
        END AS channel,
        idvs.application_id,
        CAST(NULL AS string) AS ally_id, -- Falta
        apps.ally_slug AS ally,
        apps.client_id AS prospect_id,
        idvs.idv_status AS idv_status,
        idvs.identitywastarted_at AS identitywastarted_at,
        idvs.identitywaapproved_at AS identitywaapproved_at,
        idvs.identitywarejected_at AS identitywarejected_at,
        idvs.identitywadiscarded_at AS identitywadiscarded_at,
        CASE WHEN idvs.idv_status = 'approved' THEN 1 else 0 END AS is_approved,
        CASE WHEN idvs.idv_status = 'rejected' THEN 1 else 0 END AS is_rejected,
        CASE WHEN idvs.idv_status = 'discarded' THEN 1 else 0 END AS is_discarded,
        CASE WHEN idvs.idv_status = 'pending' THEN 1 else 0 END AS is_pending,
        apps.requested_amount AS ticket,
        idvs.observations AS observations,
        imus.email AS analyst,
        CASE
            WHEN idvs.idv_status = 'approved' THEN round(extract(HOUR FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
            WHEN idvs.idv_status = 'rejected' THEN round(extract(HOUR FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
            WHEN idvs.idv_status = 'discarded' THEN round(extract(HOUR FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
        END AS aht_hours,
        CASE
            WHEN idvs.idv_status = 'approved' THEN round(extract(MINUTE FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
            WHEN idvs.idv_status = 'rejected' THEN round(extract(MINUTE FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
            WHEN idvs.idv_status = 'discarded' THEN round(extract(MINUTE FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
        END AS aht_minutes,
        CASE
            WHEN idvs.idv_status = 'approved' THEN round(extract(SECOND FROM (idvs.identitywaapproved_at-idvs.identitywastarted_at)),2)
            WHEN idvs.idv_status = 'rejected' THEN round(extract(SECOND FROM (idvs.identitywarejected_at-idvs.identitywastarted_at)),2)
            WHEN idvs.idv_status = 'discarded' THEN round(extract(SECOND FROM (idvs.identitywadiscarded_at-idvs.identitywastarted_at)),2)
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
            *,
            CASE
                WHEN identitywaapproved_at IS NOT NULL THEN 'approved'
                WHEN identitywarejected_at IS NOT NULL THEN 'rejected'
                WHEN (identitywadiscarded_at IS NOT NULL OR identitywadiscardedbyrisk_at IS NOT NULL) AND (identitywadiscarded_at > identitywastarted_at OR identitywadiscardedbyrisk_at > identitywastarted_at) THEN 'discarded'
                ELSE 'pending'
            END AS idv_status
        FROM silver.f_idv_stage_co
        WHERE identitywastarted_at IS NOT NULL
        LIMIT 10) AS idvs

    LEFT JOIN silver.f_applications_co AS apps
      ON idvs.application_id = apps.application_id
    LEFT JOIN silver.d_identity_management_users_co imus
      ON idvs.agent_user_id = imus.id
    WHERE apps.client_type = 'PROSPECT'
    ) raw
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21