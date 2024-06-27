{{
    config(
        materialized='incremental',
        unique_key='application_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with prospect_br_credit_evaluation_results_br AS (
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
from {{ ref('prospect_br_credit_evaluation_results_br') }}
{% if is_incremental() %}
    WHERE created_at BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_pre_approvals_br_credit_evaluation_results_br AS (
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
from {{ ref('f_pre_approvals_br_credit_evaluation_results_br') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_prospect_br_credit_evaluation_results_br_of AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_prospect_br_credit_evaluation_results_br') }}
where payload:['evaluation_result']['type'] = 'BNPL'
{% if is_incremental() %}
    and created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

FreshLookPolicyBooleanDecisionUnit AS (
select context_id as application_id,
       payload:['decision']['reason']['pd_score'] as final_pd,
       payload:['decision']['reason']['fpd_score'] as final_fpd,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_risk_service_decision_unit_logs_br') }} dul
where decision_unit = 'FreshLookPolicyBooleanDecisionUnit'
{% if is_incremental() %}
    and created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_prospect_br_prospect_evaluation_results_br AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_prospect_br_prospect_evaluation_results_br') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_pre_approvals_br_pre_approval_evaluation_results_br AS (
select  application_id,
        response:['preapprovalPolicyName'] as preapproval_credit_policy_name,
        response:['npvEvaluationChargeoffRate'] as preapproval_addi_pd,
        row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_pre_approvals_br_pre_approval_evaluation_results_br') }}
),

final_table as (
select  
      apps.application_id,
      coalesce(cct.pd_model_name,
                lct.pd_model_name,
                pacer.pd_model_name) as pd_model_name,
      coalesce(cct.original_pd,
                lct.original_pd,
                pacer.original_pd) as original_pd_score,
      coalesce(cct.final_pd,
                lct.final_pd,
                pacer.final_pd,
                flpbdu.final_pd) as final_pd_score,
      coalesce(cct.pd_multiplier,
                lct.pd_multiplier,
                pacer.pd_multiplier) as pd_multiplier,
      coalesce(cct.pd_no_multipliers,
                lct.pd_no_multipliers,
                pacer.pd_no_multipliers) as pd_no_multipliers,
      coalesce(per.fpd_result, flpbdu.final_fpd) as final_fpd_score,
      coalesce(cct.credit_policy_name,
               lct.credit_policy_name,
               pacer.credit_policy_name) as credit_policy_name,
      preapper.preapproval_credit_policy_name,
      pacer.original_pd as preapproval_addi_pd
from {{ ref('f_applications_br') }} apps
left join prospect_br_credit_evaluation_results_br lct
on apps.application_id = lct.application_id and lct.rn = 1
left join f_prospect_br_credit_evaluation_results_br_of cct
on apps.application_id = cct.application_id and cct.rn = 1
left join f_pre_approvals_br_credit_evaluation_results_br pacer
on apps.application_id = pacer.application_id and pacer.rn = 1
left join f_prospect_br_prospect_evaluation_results_br per
on apps.application_id = per.application_id and per.rn = 1
left join FreshLookPolicyBooleanDecisionUnit flpbdu
on apps.application_id = flpbdu.application_id and flpbdu.rn = 1
left join f_pre_approvals_br_pre_approval_evaluation_results_br preapper
on apps.application_id = preapper.application_id and preapper.rn = 1
where 1=1
and (lct.application_id is not null
or cct.application_id is not null
or pacer.application_id is not null
or per.application_id is not null
or flpbdu.application_id is not null))
select *
from final_table;

