{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

select
----------------CORE VARIABLES
ls.loan_id,
a.application_date,
ls.origination_date,
a.application_id,
a.id_number,
a.application_email,
a.application_cellphone,
dul.fraud_model_score,
dul.fraud_model_version,
bc.id_exp_city,
case when bc.cellphone_match = 0 then 1 else 0 end as phone_not_match,
case when bc.email_match = 0 then 1 else 0 end as email_not_match,
case when bc.diff_past_emails > 0 then bc.diff_past_emails else 0 end as diff_app_email,
case when bc.diff_past_cellphones > 0 then bc.diff_past_cellphones else 0 end as diff_app_cellphone,
case when instr(lower(a.application_email),lower(substring(ci.prospect_first_name,1,3))) + instr(lower(a.application_email),lower(substring(ci.prospect_middle_name,1,3))) 
+ instr(lower(a.application_email),lower(substring(ci.prospect_last_name,1,3))) + instr(lower(a.application_email),lower(substring(ci.prospect_snd_last_name,1,3))) > 0 
or ci.prospect_first_name is null then 0 else 1 end as email_not_match_name,
case when re.repeats > 0 then re.repeats else 0 end as email_repeat,
case when rc.repeats > 0 then rc.repeats else 0 end as cellphone_repeat,
case when ri.repeats > 0 then ri.repeats else 0 end as ip_repeat,
ca.weekly_apps,
case when ca.weekly_apps > 2 and ca.lesser_weekly_apps > 0 then 1 else 0 end as diff_apps_n_amount_weekly,
case when ca.monthly_apps > 2 and ca.lesser_monthly_apps > 0 then 1 else 0 end as diff_apps_n_amount_monthly,
case when ca.diff_ally_n_amount_weekly > 0 then ca.diff_ally_n_amount_weekly else 0 end as diff_ally_n_amount_weekly,
case when ca.diff_ally_n_amount_monthly > 0 then ca.diff_ally_n_amount_monthly else 0 end as diff_ally_n_amount_monthly,
case when ca.diff_ally_less_amount_weekly > 0 then ca.diff_ally_less_amount_weekly else 0 end as diff_ally_less_amount_weekly,
case when ca.diff_ally_less_amount_monthly > 0 then ca.diff_ally_less_amount_monthly else 0 end as diff_ally_less_amount_monthly,
case when ca.less_amount_weekly > 0 then ca.less_amount_weekly else 0 end as less_amount_weekly,
case when ca.less_amount_monthly > 0 then ca.less_amount_monthly else 0 end as less_amount_monthly,
case when ca.diff_vertical_weekly > 0 then ca.diff_vertical_weekly else 0 end as diff_vertical_weekly,
case when ca.diff_vertical_monthly > 0 then ca.diff_vertical_monthly else 0 end as diff_vertical_monthly,
case when ca.preapp_amount is null and ca.second_vs_first > 2 then ca.second_vs_first else 0 end as snd_loan_2x,
case when ca.p_apps7days > 0.9 then ca.p_apps7days else 0 end as full_preapp_amount,
ra.repeated_address,
cpem.clean_email_prev_changed,
cpem.clean_email_prev_used,
cpem.clean_email,
length(cpem.clean_email) as clean_email_len,
btwd.mr_walkdown,
pa.preapp_hesitation,
pa.preapp_to_orig_minutes,
pa.prev_app_minutes,
kyc.app_cellphone_in_bureau_first_report,
kyc.app_cellphone_in_bureau_last_report,
kyc.cellphones_first_report,
kyc.cellphones_last_report,
----------------THIRD-PARTY VARIABLES
----Emailage
em.em_email,
em.em_advice,
em.em_score,
em.em_domain_creation_days,
em.em_reason,
em.em_status,
em.em_risk_band,
em.em_last_verification_date,
em.em_first_verification_date,
datediff(orig.origination_date::date, em.em_first_verification_date) as em_fvd_recency,
datediff(orig.origination_date::date, em.em_last_verification_date) as em_lvd_recency,
----NeuroId
nid.nid_email_autofill,
nid.nid_email_dataImport,
nid.nid_email_hesitation,
nid.nid_email_manipulation,
nid.nid_email_frictionIndex,
nid.nid_email_interactionTime,
nid.nid_email_timeToFirstAnswer,
nid.nid_email_repeatInteractions,
--nid.nid_fullName_autofill,
--nid.nid_fullName_dataImport,
--nid.nid_fullName_hesitation,
--nid.nid_fullName_manipulation,
--nid.nid_fullName_frictionIndex,
--nid.nid_fullName_interactionTime,
--nid.nid_fullName_timeToFirstAnswer,
--nid.nid_fullName_repeatInteractions,
nid.nid_lastName_autofill,
nid.nid_lastName_dataImport,
nid.nid_lastName_hesitation,
nid.nid_lastName_manipulation,
nid.nid_lastName_frictionIndex,
nid.nid_lastName_interactionTime,
nid.nid_lastName_timeToFirstAnswer,
nid.nid_lastName_repeatInteractions,
nid.nid_cellphoneNumber_autofill,
nid.nid_cellphoneNumber_dataImport,
nid.nid_cellphoneNumber_hesitation,
nid.nid_cellphoneNumber_manipulation,
nid.nid_cellphoneNumber_frictionIndex,
nid.nid_cellphoneNumber_interactionTime,
nid.nid_cellphoneNumber_timeToFirstAnswer,
nid.nid_cellphoneNumber_repeatInteractions,
nid.nid_nationalIdentificationNumber_autofill,
nid.nid_nationalIdentificationNumber_dataImport,
nid.nid_nationalIdentificationNumber_hesitation,
nid.nid_nationalIdentificationNumber_manipulation,
nid.nid_nationalIdentificationNumber_frictionIndex,
nid.nid_nationalIdentificationNumber_interactionTime,
nid.nid_nationalIdentificationNumber_timeToFirstAnswer,
nid.nid_nationalIdentificationNumber_repeatInteractions,
nid.nid_nationalExpeditionDateDay_autofill,
nid.nid_nationalExpeditionDateDay_dataImport,
nid.nid_nationalExpeditionDateDay_hesitation,
nid.nid_nationalExpeditionDateDay_manipulation,
nid.nid_nationalExpeditionDateDay_frictionIndex,
nid.nid_nationalExpeditionDateDay_interactionTime,
nid.nid_nationalExpeditionDateDay_timeToFirstAnswer,
nid.nid_nationalExpeditionDateDay_repeatInteractions,
nid.nid_nationalExpeditionDateYear_autofill,
nid.nid_nationalExpeditionDateYear_dataImport,
nid.nid_nationalExpeditionDateYear_hesitation,
nid.nid_nationalExpeditionDateYear_manipulation,
nid.nid_nationalExpeditionDateYear_frictionIndex,
nid.nid_nationalExpeditionDateYear_interactionTime,
nid.nid_nationalExpeditionDateYear_timeToFirstAnswer,
nid.nid_nationalExpeditionDateYear_repeatInteractions,
nid.nid_nationalExpeditionDateMonth_autofill,
nid.nid_nationalExpeditionDateMonth_dataImport,
nid.nid_nationalExpeditionDateMonth_hesitation,
nid.nid_nationalExpeditionDateMonth_manipulation,
nid.nid_nationalExpeditionDateMonth_frictionIndex,
nid.nid_nationalExpeditionDateMonth_interactionTime,
nid.nid_nationalExpeditionDateMonth_timeToFirstAnswer,
nid.nid_nationalExpeditionDateMonth_repeatInteractions,
nid.nid_sessionBreaks,
nid.nid_totalSessionIdleTime,
nid.nid_totalSessionInteractionTime,
----Telesign
ts.ts_status_description,
ts.ts_sim_swap_date,
ts.ts_carrier_name,
----Iovation
io.io_result,
io.io_reason,
io.io_tracking_number,
io.io_device_os,
io.io_device_type,
io.io_device_alias,
io.io_device_screen,
io.io_device_browser_type,
io.io_device_browser_version,
io.io_device_firstSeen,
io.io_stated_ip_isp,
io.io_stated_ip_address,
io.io_stated_ip_ipLocation_city,
io.io_stated_ip_ipLocation_region,
io.io_stated_ip_ipLocation_country,
io.io_real_ip_isp,
io.io_real_ip_address,
io.io_real_ip_ipLocation_city,
io.io_real_ip_ipLocation_region,
io.io_real_ip_ipLocation_country,
io.io_rule_results_rules,
io.io_rule_results_score,
io.io_rule_results_rulesMatched,
io.io_rejection_rule_results_rules,
io.io_rejection_rule_results_score,
io.io_rejection_rule_results_rulesMatched
from {{ source('silver_live', 'f_pii_applications_co') }} a
left join {{ ref('fmt_fpd_models_co') }} dul on a.application_id = dul.application_id
left join {{ source('silver_live', 'f_originations_bnpl_co') }} orig on a.application_id = orig.application_id
left join {{ ref('dm_loan_status_co') }} ls on orig.loan_id = ls.loan_id
left join {{ ref('fmt_client_info_co') }} ci on a.client_id = ci.prospect_id 
left join {{ ref('fmt_emailage_co') }} em on a.application_id = em.application_id
left join {{ ref('fmt_neuroid_co') }} nid on a.application_id = nid.application_id
left join {{ ref('fmt_telesign_co') }} ts on a.application_id = ts.application_id
left join {{ ref('fmt_iovation_co') }} io on a.application_id = io.application_id
left join {{ ref('fmt_kyc_all_info_co') }} kyc on a.application_id = kyc.application_id
left join {{ ref('fmt_bureau_check_co') }} bc on a.application_id = bc.application_id
left join {{ ref('fmt_repeat_email_co') }} re on a.application_id = re.application_id
left join {{ ref('fmt_repeat_cellphone_co') }} rc on a.application_id = rc.application_id
left join {{ ref('fmt_repeat_ip_address_co') }} ri on a.application_id = ri.application_id
left join {{ ref('fmt_changed_app_co') }} ca on a.application_id = ca.application_id 
left join {{ ref('fmt_repeated_address_co') }} ra on a.application_id = ra.application_id
left join {{ ref('fmt_clean_phone_email_match_co') }} cpem on a.application_id = cpem.application_id
left join {{ ref('fmt_bt_walk_down_co') }} btwd on a.application_id = btwd.application_id
left join {{ ref('fmt_prev_app_information_co') }} pa on a.application_id = pa.application_id
where a.application_date >= '2021-08-01'
