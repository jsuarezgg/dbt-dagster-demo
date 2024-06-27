

with prospect_co_credit_evaluation_results_co AS (
select application_id,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_name'] as pd_model_name,
       evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'] as original_pd,
       evaluation_result:['pd_models']['applied_pd_model']['result'] as final_pd,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'] as pd_multiplier,
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'],
           evaluation_result:['pd_models']['applied_pd_model']['result']
           ) /
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'],
           1
           ) as pd_no_multipliers,
       evaluation_result:['segment']['policy_name'] as credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from bronze.prospect_co_credit_evaluation_results_co

    WHERE created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

prospect_pago_co_credit_evaluation_results_co AS (
select application_id,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_name'] as pd_model_name,
       evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'] as original_pd,
       evaluation_result:['pd_models']['applied_pd_model']['result'] as final_pd,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'] as pd_multiplier,
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'],
           evaluation_result:['pd_models']['applied_pd_model']['result']
           ) /
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'],
           1
           ) as pd_no_multipliers,
       evaluation_result:['segment']['policy_name'] as credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from bronze.prospect_pago_co_credit_evaluation_results_co

    WHERE created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_pre_approvals_co_credit_evaluation_results_co AS (
select application_id,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_name'] as pd_model_name,
       evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'] as original_pd,
       evaluation_result:['pd_models']['applied_pd_model']['result'] as final_pd,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'] as pd_multiplier,
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'],
           evaluation_result:['pd_models']['applied_pd_model']['result']
           ) /
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'],
           1
           ) as pd_no_multipliers,
       evaluation_result:['segment']['policy_name'] as credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from silver.f_pre_approvals_co_credit_evaluation_results_co

    WHERE created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_returning_clients_co_credit_evaluation_results_co AS (
select application_id,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_name'] as pd_model_name,
       evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'] as original_pd,
       evaluation_result:['pd_models']['applied_pd_model']['result'] as final_pd,
       evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'] as pd_multiplier,
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['result_without_psychometric_assessment_multiplier'],
           evaluation_result:['pd_models']['applied_pd_model']['result']
           ) /
       coalesce(
           evaluation_result:['pd_models']['applied_pd_model']['pd_model_multiplier'],
           1
           ) as pd_no_multipliers,
       evaluation_result:['segment']['policy_name'] as credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from silver.f_returning_clients_co_credit_evaluation_results_co

    WHERE created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_prospect_co_credit_evaluation_results_co_of AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from silver.f_prospect_co_credit_evaluation_results_co
where payload:['evaluation_result']['type'] = 'BNPL'

    and created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_prospect_pago_co_credit_evaluation_results_co_of AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from silver.f_prospect_pago_co_credit_evaluation_results_co
where payload:['evaluation_result']['type'] = 'BNPL'

    and created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_returning_clients_pago_co_credit_evaluation_results_co AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from silver.f_returning_clients_pago_co_credit_evaluation_results_co
where payload:['evaluation_result']['type'] = 'BNPL'

    and created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

FreshLookPolicyBooleanDecisionUnit AS (
select context_id as application_id,
       payload:['decision']['reason']['pd_score'] as final_pd,
       payload:['decision']['reason']['fpd_score'] as final_fpd,
       row_number() over(partition by context_id order by created_at desc) as rn
from silver.f_risk_service_decision_unit_logs_co dul
where decision_unit = 'FreshLookPolicyBooleanDecisionUnit'

    and created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_prospect_co_prospect_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       row_number() over(partition by application_id order by created_at desc) as rn
from silver.f_prospect_co_prospect_evaluation_results_co

    WHERE created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

f_prospect_pago_co_prospect_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       row_number() over(partition by application_id order by created_at desc) as rn
from silver.f_prospect_pago_co_prospect_evaluation_results_co

    WHERE created_at BETWEEN (to_date("2022-01-01"- INTERVAL 3 day)) AND to_date("2022-01-30")
),

legacy_cer_tables as (
select *
from prospect_co_credit_evaluation_results_co
where rn = 1
union all
select *
from prospect_pago_co_credit_evaluation_results_co
where rn = 1
union all
select *
from f_pre_approvals_co_credit_evaluation_results_co
where rn = 1
union all
select *
from f_returning_clients_co_credit_evaluation_results_co
where rn = 1
), 

current_cer_tables as (
select *
from f_prospect_co_credit_evaluation_results_co_of
where rn = 1
union all
select *
from f_prospect_pago_co_credit_evaluation_results_co_of
where rn = 1
union all
select *
from f_returning_clients_pago_co_credit_evaluation_results_co
where rn = 1
),

current_per_tables as (
select *
from f_prospect_co_prospect_evaluation_results_co
where rn = 1
union all
select *
from f_prospect_pago_co_prospect_evaluation_results_co
where rn = 1
),

final_table as (
select  
      apps.application_id,
      coalesce(cct.pd_model_name,
                lct.pd_model_name) as pd_model_name,
      coalesce(cct.original_pd,
                lct.original_pd) as original_pd_score,
      coalesce(cct.final_pd,
                lct.final_pd,
                flpbdu.final_pd) as final_pd_score,
      coalesce(cct.pd_multiplier,
                lct.pd_multiplier) as pd_multiplier,
      coalesce(cct.pd_no_multipliers,
                lct.pd_no_multipliers) as pd_no_multipliers,
      coalesce(per.fpd_result, flpbdu.final_fpd) as final_fpd_score,
      coalesce(cct.credit_policy_name,
               lct.credit_policy_name) as credit_policy_name
from silver.f_applications_co apps
left join legacy_cer_tables lct
on apps.application_id = lct.application_id
left join current_cer_tables cct
on apps.application_id = cct.application_id
left join current_per_tables per
on apps.application_id = per.application_id and per.rn = 1
left join FreshLookPolicyBooleanDecisionUnit flpbdu
on apps.application_id = flpbdu.application_id and flpbdu.rn = 1
where 1=1
and (lct.application_id is not null
or cct.application_id is not null
or per.application_id is not null
or flpbdu.application_id is not null))
select *
from final_table;