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
from {{ ref('prospect_co_credit_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

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
from {{ ref('prospect_pago_co_credit_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

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
from {{ ref('f_pre_approvals_co_credit_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

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
from {{ ref('f_returning_clients_co_credit_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_prospect_co_credit_evaluation_results_co_of AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_prospect_co_credit_evaluation_results_co') }}
where payload:['evaluation_result']['type'] = 'BNPL'
{% if is_incremental() %}
    and created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_prospect_pago_co_credit_evaluation_results_co_of AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_prospect_pago_co_credit_evaluation_results_co') }}
where payload:['evaluation_result']['type'] = 'BNPL'
{% if is_incremental() %}
    and created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_returning_clients_pago_co_credit_evaluation_results_co AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_returning_clients_pago_co_credit_evaluation_results_co') }}
where payload:['evaluation_result']['type'] = 'BNPL'
{% if is_incremental() %}
    and created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_returning_clients_financia_co_credit_evaluation_results_co_of AS (
select context_id as application_id,
       payload:['config']['multiplier'] as pd_multiplier,
       payload:['config']['selected_model']['name'] as pd_model_name,
       payload:['config']['selected_model']['score'] * payload:['config']['multiplier'] as final_pd,
       payload:['config']['selected_model']['score'] as pd_no_multipliers,
       payload:['config']['selected_model']['score'] as original_pd,
       payload:['config']['config_name'] as credit_policy_name,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_returning_clients_financia_co_credit_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_models_service_model_logs_co_of AS (
select context_id as application_id,
        output:score AS final_pd,
        output:name AS model_name,
        row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_models_service_model_logs_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

FreshLookPolicyBooleanDecisionUnit AS (
select context_id as application_id,
       payload:['decision']['reason']['pd_score'] as final_pd,
       payload:['decision']['reason']['fpd_score'] as final_fpd,
       row_number() over(partition by context_id order by created_at desc) as rn
from {{ ref('f_risk_service_decision_unit_logs_co') }} dul
where decision_unit = 'FreshLookPolicyBooleanDecisionUnit'
{% if is_incremental() %}
    and created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_prospect_co_prospect_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       coalesce(response:['creditScore'],
                request:['kycData']:['commercialInfo']:['scores'][0]['score']) as credit_score,
       request:['kycData']:['commercialInfo']:['scores'][0]['name'] as credit_score_name,
       response:['expectedChargeoffRate'] as bureau_pd,
       response:['creditPolicyName'] credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_prospect_co_prospect_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_prospect_pago_co_prospect_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       coalesce(response:['creditScore'],
                request:['kycData']:['commercialInfo']:['scores'][0]['score']) as credit_score, -- this will completely null
       request:['kycData']:['commercialInfo']:['scores'][0]['name'] as credit_score_name, -- this will completely null
       response:['expectedChargeoffRate'] as bureau_pd, --this will be completely null
       response:['creditPolicyName'] credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_prospect_pago_co_prospect_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_returning_clients_co_returning_client_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       request:['kycData']:['commercialInfo']:['scores'][0]['name'] as credit_score_name,
       request:['kycData']:['commercialInfo']:['scores'][0]['score'] as credit_score,
       response:['expectedChargeoffRate'] as bureau_pd,
       response:['creditPolicyName'] credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_returning_clients_co_returning_client_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_returning_clients_pago_co_returning_client_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       request:['kycData']:['commercialInfo']:['scores'][0]['name'] as credit_score_name,
       request:['kycData']:['commercialInfo']:['scores'][0]['score'] as credit_score,
       response:['expectedChargeoffRate'] as bureau_pd,
       response:['creditPolicyName'] credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_returning_clients_pago_co_returning_client_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_pre_approvals_pago_co_pre_approval_evaluation_results_co AS (
select application_id,
       response:['fpdResult'] as fpd_result,
       request:['kycData']:['commercialInfo']:['scores'][0]['name'] as credit_score_name,
       request:['kycData']:['commercialInfo']:['scores'][0]['score'] as credit_score,
       response:['expectedChargeoffRate'] as bureau_pd,
       response:['creditPolicyName'] credit_policy_name,
       row_number() over(partition by application_id order by created_at desc) as rn
from {{ ref('f_pre_approvals_pago_co_pre_approval_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

f_pre_approvals_pago_co_credit_evaluation_results_co AS (
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
       from {{ ref('f_pre_approvals_pago_co_credit_evaluation_results_co') }}
{% if is_incremental() %}
    WHERE created_at::date BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL 3 day)) AND to_date("{{ var('end_date') }}")
{% endif %}),

legacy_cer_tables as (
select
    "prospect_co_credit_evaluation_results_co" as source_table,
    application_id,
    pd_model_name,
    original_pd,
    final_pd,
    pd_multiplier,
    pd_no_multipliers,
    credit_policy_name,
    rn
from prospect_co_credit_evaluation_results_co
where rn = 1
union all
select
    "prospect_pago_co_credit_evaluation_results_co" as source_table,
    application_id,
    pd_model_name,
    original_pd,
    final_pd,
    pd_multiplier,
    pd_no_multipliers,
    credit_policy_name,
    rn
from prospect_pago_co_credit_evaluation_results_co
where rn = 1
union all
select
    "f_pre_approvals_co_credit_evaluation_results_co" as source_table,
    application_id,
    pd_model_name,
    original_pd,
    final_pd,
    pd_multiplier,
    pd_no_multipliers,
    credit_policy_name,
    rn
from f_pre_approvals_co_credit_evaluation_results_co
where rn = 1
union all
select
    "f_returning_clients_co_credit_evaluation_results_co" as source_table,
    application_id,
    pd_model_name,
    original_pd,
    final_pd,
    pd_multiplier,
    pd_no_multipliers,
    credit_policy_name,
    rn
from f_returning_clients_co_credit_evaluation_results_co
where rn = 1
), 
current_cer_tables as (
select
    "f_prospect_co_credit_evaluation_results_co_of" as source_table,
    application_id,
    pd_multiplier,
    pd_model_name,
    final_pd,
    pd_no_multipliers,
    original_pd,
    credit_policy_name,
    rn
from f_prospect_co_credit_evaluation_results_co_of
where rn = 1
union all
select
    "f_prospect_pago_co_credit_evaluation_results_co_of" as source_table,
    application_id,
    pd_multiplier,
    pd_model_name,
    final_pd,
    pd_no_multipliers,
    original_pd,
    credit_policy_name,
    rn
from f_prospect_pago_co_credit_evaluation_results_co_of
where rn = 1
union all
select
    "f_returning_clients_pago_co_credit_evaluation_results_co" as source_table,
    application_id,
    pd_multiplier,
    pd_model_name,
    final_pd,
    pd_no_multipliers,
    original_pd,
    credit_policy_name,
    rn
from f_returning_clients_pago_co_credit_evaluation_results_co
where rn = 1
union all
select
    "f_returning_clients_financia_co_credit_evaluation_results_co_of" as source_table,
    application_id,
    pd_multiplier,
    pd_model_name,
    final_pd,
    pd_no_multipliers,
    original_pd,
    credit_policy_name,
    rn
from f_returning_clients_financia_co_credit_evaluation_results_co_of
where rn = 1
union all
select
    "f_pre_approvals_pago_co_credit_evaluation_results_co" as source_table,
    application_id,
    pd_multiplier,
    pd_model_name,
    final_pd,
    pd_no_multipliers,
    original_pd,
    credit_policy_name,
    rn
from f_pre_approvals_pago_co_credit_evaluation_results_co
where rn = 1
),
current_per_tables as (
select
    "f_prospect_co_prospect_evaluation_results_co" as source_table,
    application_id,
    fpd_result,
    credit_score,
    credit_score_name,
    credit_policy_name,
    bureau_pd,
    rn
from f_prospect_co_prospect_evaluation_results_co
where rn = 1
union all
select 
    "f_prospect_pago_co_prospect_evaluation_results_co" as source_table,
    application_id,
    fpd_result,
    credit_score,
    credit_score_name,
    credit_policy_name,
    bureau_pd,
    rn
from f_prospect_pago_co_prospect_evaluation_results_co
where rn = 1
union all
select 
    "f_returning_clients_co_returning_client_evaluation_results_co" as source_table,
    application_id,
    fpd_result,
    credit_score,
    credit_score_name,
    credit_policy_name,
    bureau_pd,
    rn
from f_returning_clients_co_returning_client_evaluation_results_co
where rn = 1
union all
select 
    "f_returning_clients_pago_co_returning_client_evaluation_results_co" as source_table,
    application_id,
    fpd_result,
    credit_score,
    credit_score_name,
    credit_policy_name,
    bureau_pd,
    rn
from f_returning_clients_pago_co_returning_client_evaluation_results_co
where rn = 1
union all
select 
    "f_pre_approvals_pago_co_pre_approval_evaluation_results_co" as source_table,
    application_id,
    fpd_result,
    credit_score,
    credit_score_name,
    credit_policy_name,
    bureau_pd,
    rn
from f_pre_approvals_pago_co_pre_approval_evaluation_results_co
where rn = 1
),

final_table as (
select  
      apps.application_id,
      coalesce(cct.source_table,
               lct.source_table,
               per.source_table) as source_table,
      coalesce(cct.pd_model_name,
                lct.pd_model_name,
                ffmsml.model_name) as pd_model_name,
      coalesce(cct.original_pd,
                lct.original_pd) as original_pd_score,
      coalesce(cct.final_pd,
                lct.final_pd,
                flpbdu.final_pd,
                ffmsml.final_pd) as final_pd_score,
      coalesce(cct.pd_multiplier,
                lct.pd_multiplier) as pd_multiplier,
      coalesce(cct.pd_no_multipliers,
                lct.pd_no_multipliers) as pd_no_multipliers,
      coalesce(per.fpd_result, flpbdu.final_fpd) as final_fpd_score,
      coalesce(cct.credit_policy_name,
               lct.credit_policy_name,
               per.credit_policy_name) as credit_policy_name,
      per.credit_score as credit_score,
      per.credit_score_name as credit_score_name,
      per.bureau_pd as bureau_pd
from {{ source('silver_live', 'f_applications_co') }} apps
left join legacy_cer_tables lct
on apps.application_id = lct.application_id
left join current_cer_tables cct
on apps.application_id = cct.application_id
left join current_per_tables per
on apps.application_id = per.application_id and per.rn = 1
left join FreshLookPolicyBooleanDecisionUnit flpbdu
on apps.application_id = flpbdu.application_id and flpbdu.rn = 1
left join f_models_service_model_logs_co_of ffmsml
on apps.application_id = ffmsml.application_id and ffmsml.rn = 1
where 1=1
and (lct.application_id is not null
or cct.application_id is not null
or per.application_id is not null
or flpbdu.application_id is not null
or ffmsml.application_id is not null))
select *
from final_table;