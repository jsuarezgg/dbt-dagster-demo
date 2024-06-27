WITH

-- IDV Logs tables

f_idv_stage_co_logs AS (

    SELECT
        *,
        "CO" AS country 
    FROM silver.f_idv_stage_co_logs

),

f_idv_stage_br_logs AS (

    SELECT
        *,
        "BR" AS country 
    FROM silver.f_idv_stage_br_logs

),

base AS (

SELECT
    *,
    CASE
        WHEN event_name = 'IdentityPhotosStarted' THEN 'StartProcess'
        WHEN event_name IN ('IdentityPhotosApproved', 'IdentityPhotosRejected',
                            'IdentityPhotosDiscarded', 'IdentityPhotosDiscardedByRisk') THEN 'Finalization'
        ELSE 'Collection'
    END AS type_event,
    LAG(CASE
        WHEN event_name = 'IdentityPhotosStarted' THEN 'StartProcess'
        WHEN event_name IN ('IdentityPhotosApproved', 'IdentityPhotosRejected',
                            'IdentityPhotosDiscarded', 'IdentityPhotosDiscardedByRisk') THEN 'Finalization'
        ELSE 'Collection'
    END) OVER (PARTITION BY application_id ORDER BY ocurred_on) AS previous_event
FROM (
    SELECT * FROM f_idv_stage_co_logs
        UNION ALL
    SELECT * FROM f_idv_stage_br_logs)
),

mark_applications AS (

    SELECT
        event_id,
        1 AS number_attempt
    FROM base WHERE event_name = 'IdentityPhotosStarted'

),

mark_attempts AS (

    SELECT
        event_id,
        1 + row_number() over (PARTITION BY application_id ORDER BY ocurred_on) AS number_attempt_1
    FROM base
    WHERE type_event = 'Collection' AND previous_event = 'Finalization'

),


final AS (
SELECT
    b.*,
    last_value(coalesce(ma.number_attempt, mat.number_attempt_1), true) OVER (PARTITION BY application_id ORDER BY ocurred_on) AS number_attempt
FROM base b
LEFT JOIN mark_applications ma ON b.event_id = ma.event_id
LEFT JOIN mark_attempts mat ON b.event_id = mat.event_id

),

IdentityPhotosStarted AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosStarted
FROM final
WHERE event_name = 'IdentityPhotosStarted'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosCollected AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosCollected
FROM final
WHERE event_name = 'IdentityPhotosCollected'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosEvaluationStarted AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosEvaluationStarted
FROM final
WHERE event_name = 'IdentityPhotosEvaluationStarted'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosAgentAssigned AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosAgentAssigned
FROM final
WHERE event_name = 'IdentityPhotosAgentAssigned'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosKeptByAgent AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosKeptByAgent
FROM final
WHERE event_name = 'IdentityPhotosKeptByAgent'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosDiscarded AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosDiscarded
FROM final
WHERE event_name = 'IdentityPhotosDiscarded'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosDiscardedByRisk AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosDiscardedByRisk
FROM final
WHERE event_name = 'IdentityPhotosDiscardedByRisk'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosApproved AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosApproved
FROM final
WHERE event_name = 'IdentityPhotosApproved'
GROUP BY client_id, application_id, number_attempt

),

IdentityPhotosRejected AS (

SELECT
    max(country) AS country,
    client_id,
    application_id,
    number_attempt,
    max(idv_provider) AS idv_provider,
    max(agent_user_id) AS agent_user_id,
    max(ip_address) AS ip_address,
    max(user_agent) AS user_agent,
    max(is_mobile) AS is_mobile,
    max(is_high_risk) AS is_high_risk,
    max(reason) AS reason,
    max(used_policy_id) AS used_policy_id,
    max(observations) AS observations,
    max(ocurred_on) AS IdentityPhotosRejected
FROM final
WHERE event_name = 'IdentityPhotosRejected'
GROUP BY client_id, application_id, number_attempt

)

SELECT
    c.country,
    c.client_id,
    c.application_id,
    c.number_attempt,
    coalesce(r.idv_provider, ap.idv_provider, dr.idv_provider, d.idv_provider) AS idv_provider,
    a.agent_user_id AS agent_user_id,
    c.ip_address AS ip_address,
    c.user_agent AS user_agent,
    c.is_high_risk AS is_high_risk,
    c.is_mobile AS is_mobile,
    coalesce(r.observations, ap.observations, dr.observations, d.observations) AS observations,
    coalesce(r.reason, ap.reason, dr.reason, d.reason) AS reason,
    coalesce(r.used_policy_id, ap.used_policy_id, dr.used_policy_id, d.used_policy_id) AS used_policy_id,
    IdentityPhotosStarted,
    IdentityPhotosCollected,
    IdentityPhotosEvaluationStarted,
    IdentityPhotosAgentAssigned,
    IdentityPhotosKeptByAgent,
    IdentityPhotosDiscarded,
    IdentityPhotosDiscardedByRisk,
    IdentityPhotosApproved,
    IdentityPhotosRejected

FROM IdentityPhotosCollected c

LEFT JOIN IdentityPhotosStarted s
    ON c.client_id = s.client_id AND c.application_id = s.application_id

LEFT JOIN IdentityPhotosEvaluationStarted e
    ON c.client_id = e.client_id AND c.application_id = e.application_id AND c.number_attempt = e.number_attempt

LEFT JOIN IdentityPhotosAgentAssigned a
    ON c.client_id = a.client_id AND c.application_id = a.application_id AND c.number_attempt = a.number_attempt

LEFT JOIN IdentityPhotosKeptByAgent k
    ON c.client_id = k.client_id AND c.application_id = k.application_id AND c.number_attempt = k.number_attempt

LEFT JOIN IdentityPhotosDiscarded d
    ON c.client_id = d.client_id AND c.application_id = d.application_id AND c.number_attempt = d.number_attempt

LEFT JOIN IdentityPhotosDiscardedByRisk dr
    ON c.client_id = dr.client_id AND c.application_id = dr.application_id AND c.number_attempt = dr.number_attempt

LEFT JOIN IdentityPhotosApproved ap
    ON c.client_id = ap.client_id AND c.application_id = ap.application_id AND c.number_attempt = ap.number_attempt

LEFT JOIN IdentityPhotosRejected r
    ON c.client_id = r.client_id AND c.application_id = r.application_id AND c.number_attempt = r.number_attempt