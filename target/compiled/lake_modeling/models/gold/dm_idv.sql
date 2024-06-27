

WITH co_identity_started AS (
     SELECT to_date(application_date) AS period,
          application_date,
          used_policy_id AS idv_policy,
          lower(reason) AS reason,
          case when upper(journey_name) like '%FINANCIA%' and upper(journey_name) like '%SANTANDER%' then 'SANTANDER_CO'
               when upper(journey_name) like '%FINANCIA%' then 'FINANCIA_CO' 
               else product 
          end channel,
          application_id,
          ally_id,
          lower(ally_name) AS ally,
          prospect_id,
          idv_status AS idv_status,
          identitywastarted_at,
          identitywaapproved_at,
          identitywarejected_at,
          identitywadiscarded_at,
          CASE WHEN idv_status = 'approved' THEN 1 else 0 END AS is_approved,
          CASE WHEN idv_status = 'rejected' THEN 1 else 0 END AS is_rejected,
          CASE WHEN idv_status in ('discarded', 'discarded_by_risk') THEN 1 else 0 END AS is_discarded,
          CASE WHEN idv_status not in ('approved', 'rejected', 'discarded','discarded_by_risk') THEN 1 else 0 END AS is_pending,
          requested_amount AS ticket,
          u.email AS analyst,
          CASE WHEN idv_status = 'approved' THEN round(extract(HOUR FROM (identitywaapproved_at-identitywastarted_at)),2)
               WHEN idv_status = 'rejected' THEN round(extract(HOUR FROM (identitywarejected_at-identitywastarted_at)),2)
               WHEN idv_status = 'discarded' THEN round(extract(HOUR FROM (identitywadiscarded_at-identitywastarted_at)),2) 
          END AS aht_hours,
          CASE WHEN idv_status = 'approved' THEN round(extract(MINUTE FROM (identitywaapproved_at-identitywastarted_at)),2)
               WHEN idv_status = 'rejected' THEN round(extract(MINUTE FROM (identitywarejected_at-identitywastarted_at)),2)
               WHEN idv_status = 'discarded' THEN round(extract(MINUTE FROM (identitywadiscarded_at-identitywastarted_at)),2) 
          END AS aht_minutes,
          CASE WHEN idv_status = 'approved' THEN round(extract(SECOND FROM (identitywaapproved_at-identitywastarted_at)),2)
               WHEN idv_status = 'rejected' THEN round(extract(SECOND FROM (identitywarejected_at-identitywastarted_at)),2)
               WHEN idv_status = 'discarded' THEN round(extract(SECOND FROM (identitywadiscarded_at-identitywastarted_at)),2) 
          END AS aht_seconds,
          round(extract(HOUR FROM (identitywaapproved_at-identitywastarted_at)),2) AS aht_hours_approval,
          round(extract(HOUR FROM (identitywarejected_at-identitywastarted_at)),2) AS aht_hours_rejected,
          round(extract(HOUR FROM (identitywadiscarded_at-identitywastarted_at)),2) AS aht_hours_discarded,
          round(extract(MINUTE FROM (identitywaapproved_at-identitywastarted_at)),2) AS aht_minutes_approval,
          round(extract(MINUTE FROM (identitywarejected_at-identitywastarted_at)),2) AS aht_minutes_rejected,
          round(extract(MINUTE FROM (identitywadiscarded_at-identitywastarted_at)),2) AS aht_minutes_discarded,
          round(extract(SECOND FROM (identitywaapproved_at-identitywastarted_at)),2) AS aht_seconds_approval,
          round(extract(SECOND FROM (identitywarejected_at-identitywastarted_at)),2) AS aht_seconds_rejected,
          round(extract(SECOND FROM (identitywadiscarded_at-identitywastarted_at)),2) AS aht_seconds_discarded
     FROM cur.applications a
     LEFT JOIN bronze.identity_management_users_co u on u.id = a.agent_user_id
     where identitywastarted_at is not null and client_type = 'PROSPECT'
), 
co_identity_started_grouped as (
     SELECT co_identity_started.period,
          'CO' as country_code,
          'idv_2.0' as idv_version,
          co_identity_started.application_date,
          co_identity_started.application_id,
          co_identity_started.idv_status,
          co_identity_started.identitywastarted_at,
          co_identity_started.identitywaapproved_at,
          co_identity_started.identitywarejected_at,
          co_identity_started.identitywadiscarded_at,
          null AS identityphotosevaluationstarted_at,
          co_identity_started.analyst,
          co_identity_started.channel,
          co_identity_started.idv_policy,
          co_identity_started.reason,
          co_identity_started.ally_id,
          co_identity_started.ally,
          co_identity_started.prospect_id,
          co_identity_started.ticket,
          count(DISTINCT co_identity_started.application_id) AS total_apps,
          count(DISTINCT co_identity_started.analyst) AS active_analyst,
          sum(co_identity_started.is_approved) AS idv_approved,
          sum(co_identity_started.is_rejected) AS idv_rejected,
          sum(co_identity_started.is_discarded) AS idv_discarded,
          sum(co_identity_started.is_pending) AS idv_pending,
          round(percentile(aht_hours*3600+aht_minutes*60+aht_seconds,0.5),2) AS aht,
          round(percentile(aht_hours_approval*3600+aht_minutes_approval*60+aht_seconds_approval,0.5),2) AS aht_approval,
          round(percentile(aht_hours_rejected*3600+aht_minutes_rejected*60+aht_seconds_rejected,0.5),2) AS aht_rejected,
          round(percentile(aht_hours_discarded*3600+aht_minutes_discarded*60+aht_seconds_discarded,0.5),2) AS aht_discarded,
          null as aht_handling,
          null AS aht_queued
     FROM co_identity_started
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),
co_subquery as (
     select  
          *,  
          case 
               when identityphotosapproved_at is not null then 'approved'
               when identityphotosrejected_at is not null then 'rejected'
               when (identityphotosdiscarded_at is not null 
                    or identityphotosdiscardedbyrisk_at is not null) then 'discarded'
               else 'pending'
          end as idv_status_new 
     from cur.applications
     where identityphotosstarted_at is not null
     and client_type = 'PROSPECT'
),
co_identity_photos_started AS (
     SELECT 
          to_date(application_date) AS period,
          application_date,
          used_policy_id AS idv_policy,
          lower(reason) AS reason,
          case when upper(journey_name) like '%FINANCIA%' and upper(journey_name) like '%SANTANDER%' then 'SANTANDER_CO'
               when upper(journey_name) like '%FINANCIA%' then 'FINANCIA_CO' 
               else product 
          end channel,
          application_id,
          ally_id,
          lower(ally_name) AS ally,
          prospect_id,
          idv_status_new AS idv_status,
          identityphotosstarted_at as identitywastarted_at,
          identityphotosapproved_at as identitywaapproved_at,
          identityphotosrejected_at as identitywarejected_at,
          identityphotosdiscarded_at as identitywadiscarded_at,
          identityphotosevaluationstarted_at,
          CASE WHEN idv_status_new = 'approved' THEN 1 else 0 END AS is_approved,
          CASE WHEN idv_status_new = 'rejected' THEN 1 else 0 END AS is_rejected,
          CASE WHEN idv_status_new = 'discarded' THEN 1 else 0 END AS is_discarded,
          CASE WHEN idv_status_new = 'pending' THEN 1 else 0 END AS is_pending,
          requested_amount AS ticket,
          u.email AS analyst,
          CASE WHEN idv_status_new = 'approved' THEN round(extract(HOUR FROM (identityphotosapproved_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'rejected' THEN round(extract(HOUR FROM (identityphotosrejected_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'discarded' THEN round(extract(HOUR FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) 
          END AS aht_hours,
          CASE WHEN idv_status_new = 'approved' THEN round(extract(MINUTE FROM (identityphotosapproved_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'rejected' THEN round(extract(MINUTE FROM (identityphotosrejected_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'discarded' THEN round(extract(MINUTE FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) 
          END AS aht_minutes,
          CASE WHEN idv_status_new = 'approved' THEN round(extract(SECOND FROM (identityphotosapproved_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'rejected' THEN round(extract(SECOND FROM (identityphotosrejected_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'discarded' THEN round(extract(SECOND FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) 
          END AS aht_seconds,
          round(extract(HOUR FROM (identityphotosapproved_at-identityphotosstarted_at)),2) AS aht_hours_approval,
          round(extract(HOUR FROM (identityphotosrejected_at-identityphotosstarted_at)),2) AS aht_hours_rejected,
          round(extract(HOUR FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) AS aht_hours_discarded,
          round(extract(HOUR FROM (identityphotosapproved_at - identityphotosevaluationstarted_at)),2) AS aht_hours_handling,
          round(extract(HOUR FROM (identityphotosevaluationstarted_at - identityphotosstarted_at)),2) AS aht_hours_queued,
          round(extract(MINUTE FROM (identityphotosapproved_at-identityphotosstarted_at)),2) AS aht_minutes_approval,
          round(extract(MINUTE FROM (identityphotosrejected_at-identityphotosstarted_at)),2) AS aht_minutes_rejected,
          round(extract(MINUTE FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) AS aht_minutes_discarded,
          round(extract(MINUTE FROM (identityphotosapproved_at - identityphotosevaluationstarted_at)),2) AS aht_minutes_handling,
          round(extract(MINUTE FROM (identityphotosevaluationstarted_at - identityphotosstarted_at)),2) AS aht_minutes_queued,
          round(extract(SECOND FROM (identityphotosapproved_at-identityphotosstarted_at)),2) AS aht_seconds_approval,
          round(extract(SECOND FROM (identityphotosrejected_at-identityphotosstarted_at)),2) AS aht_seconds_rejected,
          round(extract(SECOND FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) AS aht_seconds_discarded,
          round(extract(SECOND FROM (identityphotosapproved_at - identityphotosevaluationstarted_at)),2) AS aht_seconds_handling,
          round(extract(SECOND FROM (identityphotosevaluationstarted_at - identityphotosstarted_at)),2) AS aht_seconds_queued
     FROM co_subquery a
     LEFT JOIN bronze.identity_management_users_co u on u.id = a.agent_user_id
),
co_identity_photos_started_grouped as (
     SELECT period,
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
       analyst,
       channel,
       idv_policy,
       reason,
       ally_id,
       ally,
       prospect_id,
       ticket,
       count(DISTINCT application_id) AS total_apps,
       count(DISTINCT analyst) AS active_analyst,
       sum(is_approved) AS idv_approved,
       sum(is_rejected) AS idv_rejected,
       sum(is_discarded) AS idv_discarded,
       sum(is_pending) AS idv_pending,
       round(percentile(aht_hours*3600+aht_minutes*60+aht_seconds,0.5),2) AS aht,
       round(percentile(aht_hours_approval*3600+aht_minutes_approval*60+aht_seconds_approval,0.5),2) AS aht_approval,
       round(percentile(aht_hours_rejected*3600+aht_minutes_rejected*60+aht_seconds_rejected,0.5),2) AS aht_rejected,
       round(percentile(aht_hours_discarded*3600+aht_minutes_discarded*60+aht_seconds_discarded,0.5),2) AS aht_discarded,
       round(percentile(aht_hours_handling*3600+aht_minutes_handling*60+aht_seconds_handling,0.5),2) AS aht_handling,
       round(percentile(aht_hours_queued*3600+aht_minutes_queued*60+aht_seconds_queued,0.5),2) AS aht_queued
     FROM co_identity_photos_started
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),

br_identity_started AS (
     SELECT 
          to_date(application_date) AS period,
          application_date,
          used_policy_id AS idv_policy,
          lower(reason) AS reason,
          case when upper(journey_name) like '%FINANCIA%' and upper(journey_name) like '%SANTANDER%' then 'SANTANDER_CO'
               when upper(journey_name) like '%FINANCIA%' then 'FINANCIA_CO' 
               else product 
          end channel,
          application_id,
          ally_id,
          lower(ally_name) AS ally,
          prospect_id,
          idv_status AS idv_status,
          identitywastarted_at,
          identitywaapproved_at,
          identitywarejected_at,
          identitywadiscarded_at,
          CASE WHEN idv_status = 'approved' THEN 1 else 0 END AS is_approved,
          CASE WHEN idv_status = 'rejected' THEN 1 else 0 END AS is_rejected,
          CASE WHEN idv_status in ('discarded', 'discarded_by_risk') THEN 1 else 0 END AS is_discarded,
          CASE WHEN idv_status not in ('approved', 'rejected', 'discarded','discarded_by_risk') THEN 1 else 0 END AS is_pending,
          requested_amount AS ticket,
          u.email AS analyst,
          CASE WHEN idv_status = 'approved' THEN round(extract(HOUR FROM (identitywaapproved_at-identitywastarted_at)),2)
               WHEN idv_status = 'rejected' THEN round(extract(HOUR FROM (identitywarejected_at-identitywastarted_at)),2)
               WHEN idv_status = 'discarded' THEN round(extract(HOUR FROM (identitywadiscarded_at-identitywastarted_at)),2) 
          END AS aht_hours,
          CASE WHEN idv_status = 'approved' THEN round(extract(MINUTE FROM (identitywaapproved_at-identitywastarted_at)),2)
               WHEN idv_status = 'rejected' THEN round(extract(MINUTE FROM (identitywarejected_at-identitywastarted_at)),2)
               WHEN idv_status = 'discarded' THEN round(extract(MINUTE FROM (identitywadiscarded_at-identitywastarted_at)),2) 
          END AS aht_minutes,
          CASE WHEN idv_status = 'approved' THEN round(extract(SECOND FROM (identitywaapproved_at-identitywastarted_at)),2)
               WHEN idv_status = 'rejected' THEN round(extract(SECOND FROM (identitywarejected_at-identitywastarted_at)),2)
               WHEN idv_status = 'discarded' THEN round(extract(SECOND FROM (identitywadiscarded_at-identitywastarted_at)),2) 
          END AS aht_seconds,
          round(extract(HOUR FROM (identitywaapproved_at-identitywastarted_at)),2) AS aht_hours_approval,
          round(extract(HOUR FROM (identitywarejected_at-identitywastarted_at)),2) AS aht_hours_rejected,
          round(extract(HOUR FROM (identitywadiscarded_at-identitywastarted_at)),2) AS aht_hours_discarded,
          round(extract(MINUTE FROM (identitywaapproved_at-identitywastarted_at)),2) AS aht_minutes_approval,
          round(extract(MINUTE FROM (identitywarejected_at-identitywastarted_at)),2) AS aht_minutes_rejected,
          round(extract(MINUTE FROM (identitywadiscarded_at-identitywastarted_at)),2) AS aht_minutes_discarded,
          round(extract(SECOND FROM (identitywaapproved_at-identitywastarted_at)),2) AS aht_seconds_approval,
          round(extract(SECOND FROM (identitywarejected_at-identitywastarted_at)),2) AS aht_seconds_rejected,
          round(extract(SECOND FROM (identitywadiscarded_at-identitywastarted_at)),2) AS aht_seconds_discarded
     FROM cur_br.applications a
     LEFT JOIN bronze.identity_management_users_br u on u.id = a.agent_user_id
     where identitywastarted_at is not null and client_type = 'PROSPECT'
),
br_identity_started_grouped AS (
     SELECT raw.period,
       'BR' as country_code,
       'idv_2.0' as idv_version,
       raw.application_date,
       raw.application_id,
       raw.idv_status,
       raw.identitywastarted_at,
       raw.identitywaapproved_at,
       raw.identitywarejected_at,
       raw.identitywadiscarded_at,
       null AS identityphotosevaluationstarted_at,
       raw.analyst,
       raw.channel,
       raw.idv_policy,
       raw.reason,
       raw.ally_id,
       raw.ally,
       raw.prospect_id,
       raw.ticket,
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
       null as aht_handling,
       null AS aht_queued
     FROM br_identity_started raw 
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),
br_subquery AS (
     select  
          *,  
          case 
               when identityphotosapproved_at is not null then 'approved'
               when identityphotosrejected_at is not null then 'rejected'
               when (identityphotosdiscarded_at is not null 
                    or identityphotosdiscardedbyrisk_at is not null) then 'discarded'
               else 'pending'
          end as idv_status_new 
     from cur_br.applications
     where identityphotosstarted_at is not null
     and client_type = 'PROSPECT'
),
br_identity_photos_started AS (
     SELECT 
          to_date(application_date) AS period,
          application_date,
          used_policy_id AS idv_policy,
          lower(reason) AS reason,
          case when upper(journey_name) like '%FINANCIA%' and upper(journey_name) like '%SANTANDER%' then 'SANTANDER_CO'
               when upper(journey_name) like '%FINANCIA%' then 'FINANCIA_CO' 
               else product 
          end channel,
          application_id,
          ally_id,
          lower(ally_name) AS ally,
          prospect_id,
          idv_status_new AS idv_status,
          identityphotosstarted_at as identitywastarted_at,
          identityphotosapproved_at as identitywaapproved_at,
          identityphotosrejected_at as identitywarejected_at,
          identityphotosdiscarded_at as identitywadiscarded_at,
          identityphotosevaluationstarted_at,
          CASE WHEN idv_status_new = 'approved' THEN 1 else 0 END AS is_approved,
          CASE WHEN idv_status_new = 'rejected' THEN 1 else 0 END AS is_rejected,
          CASE WHEN idv_status_new = 'discarded' THEN 1 else 0 END AS is_discarded,
          CASE WHEN idv_status_new = 'pending' THEN 1 else 0 END AS is_pending,
          requested_amount AS ticket,
          u.email AS analyst,
          CASE WHEN idv_status_new = 'approved' THEN round(extract(HOUR FROM (identityphotosapproved_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'rejected' THEN round(extract(HOUR FROM (identityphotosrejected_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'discarded' THEN round(extract(HOUR FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) 
          END AS aht_hours,
          CASE WHEN idv_status_new = 'approved' THEN round(extract(MINUTE FROM (identityphotosapproved_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'rejected' THEN round(extract(MINUTE FROM (identityphotosrejected_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'discarded' THEN round(extract(MINUTE FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) 
          END AS aht_minutes,
          CASE WHEN idv_status_new = 'approved' THEN round(extract(SECOND FROM (identityphotosapproved_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'rejected' THEN round(extract(SECOND FROM (identityphotosrejected_at-identityphotosstarted_at)),2)
               WHEN idv_status_new = 'discarded' THEN round(extract(SECOND FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) 
          END AS aht_seconds,
          round(extract(HOUR FROM (identityphotosapproved_at-identityphotosstarted_at)),2) AS aht_hours_approval,
          round(extract(HOUR FROM (identityphotosrejected_at-identityphotosstarted_at)),2) AS aht_hours_rejected,
          round(extract(HOUR FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) AS aht_hours_discarded,
          round(extract(HOUR FROM (identityphotosapproved_at - identityphotosevaluationstarted_at)),2) AS aht_hours_handling,
          round(extract(HOUR FROM (identityphotosevaluationstarted_at - identityphotosstarted_at)),2) AS aht_hours_queued,
          round(extract(MINUTE FROM (identityphotosapproved_at-identityphotosstarted_at)),2) AS aht_minutes_approval,
          round(extract(MINUTE FROM (identityphotosrejected_at-identityphotosstarted_at)),2) AS aht_minutes_rejected,
          round(extract(MINUTE FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) AS aht_minutes_discarded,
          round(extract(MINUTE FROM (identityphotosapproved_at - identityphotosevaluationstarted_at)),2) AS aht_minutes_handling,
          round(extract(MINUTE FROM (identityphotosevaluationstarted_at - identityphotosstarted_at)),2) AS aht_minutes_queued,
          round(extract(SECOND FROM (identityphotosapproved_at-identityphotosstarted_at)),2) AS aht_seconds_approval,
          round(extract(SECOND FROM (identityphotosrejected_at-identityphotosstarted_at)),2) AS aht_seconds_rejected,
          round(extract(SECOND FROM (identityphotosdiscarded_at-identityphotosstarted_at)),2) AS aht_seconds_discarded,
          round(extract(SECOND FROM (identityphotosapproved_at - identityphotosevaluationstarted_at)),2) AS aht_seconds_handling,
          round(extract(SECOND FROM (identityphotosevaluationstarted_at - identityphotosstarted_at)),2) AS aht_seconds_queued
     FROM br_subquery a
     LEFT JOIN bronze.identity_management_users_br u on u.id = a.agent_user_id
),
br_identity_photos_started_grouped AS (
     SELECT period,
       'BR' as country_code,
       'idv_4.0' as idv_version,
       application_date,
       application_id,
       idv_status,
       identitywastarted_at,
       identitywaapproved_at,
       identitywarejected_at,
       identitywadiscarded_at,
       identityphotosevaluationstarted_at,
       analyst,
       channel,
       idv_policy,
       reason,
       ally_id,
       ally,
       prospect_id,
       ticket,
       count(DISTINCT application_id) AS total_apps,
       count(DISTINCT analyst) AS active_analyst,
       sum(is_approved) AS idv_approved,
       sum(is_rejected) AS idv_rejected,
       sum(is_discarded) AS idv_discarded,
       sum(is_pending) AS idv_pending,
       round(percentile(aht_hours*3600+aht_minutes*60+aht_seconds,0.5),2) AS aht,
       round(percentile(aht_hours_approval*3600+aht_minutes_approval*60+aht_seconds_approval,0.5),2) AS aht_approval,
       round(percentile(aht_hours_rejected*3600+aht_minutes_rejected*60+aht_seconds_rejected,0.5),2) AS aht_rejected,
       round(percentile(aht_hours_discarded*3600+aht_minutes_discarded*60+aht_seconds_discarded,0.5),2) AS aht_discarded,
       round(percentile(aht_hours_handling*3600+aht_minutes_handling*60+aht_seconds_handling,0.5),2) AS aht_handling,
       round(percentile(aht_hours_queued*3600+aht_minutes_queued*60+aht_seconds_queued,0.5),2) AS aht_queued
     FROM br_identity_photos_started
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),

result AS (
     select * from co_identity_started_grouped
     UNION 
     select * from co_identity_photos_started_grouped
     UNION 
     select * from br_identity_started_grouped
     UNION 
     select * from br_identity_photos_started_grouped
)

select * from result;