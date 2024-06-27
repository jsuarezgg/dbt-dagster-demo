



select a.application_id,
       orig.loan_id,
       case
        when a.journey_name like '%SANTANDER%' and (app_js.journey_stages like '%underwriting-co%' or app_js.journey_stages like '%underwriting-psychometric-co%') then 'FINANCIA_CO'
        when a.journey_name like '%SANTANDER%' then 'SANTANDER_CO'
        when a.product is not null and a.product like '%PAGO%' then 'PAGO_CO'
        when a.product is not null and a.product like '%FINANCIA%' then 'FINANCIA_CO'
        when a.journey_name is not null and a.journey_name like '%PAGO%' then 'PAGO_CO'
        when a.journey_name is not null and a.journey_name like '%FINANCIA%' then 'FINANCIA_CO'
        when udw.credit_policy_name in (
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
        'rejection_policy_pago') then 'PAGO_CO'
        when orig.term > 3 then 'FINANCIA_CO'
        when udw.credit_policy_name in ('closing_policy','finalization_policy', 'rejection_policy') and orig.term = 3 then 'PAGO_CO'
        when udw.credit_policy_name is null and orig.term > 3 then 'FINANCIA_CO'
        when udw.credit_policy_name is null and orig.approved_amount<=600000 then 'PAGO_CO'
        when udw.credit_policy_name is not null then 'FINANCIA_CO' else 'FINANCIA_CO'
        end as product
from silver.f_applications_co a
left join silver.f_underwriting_fraud_stage_co udw
on a.application_id = udw.application_id
left join silver.f_originations_bnpl_co orig
on a.application_id = orig.application_id
left join gold.rmt_application_journey_stages_co app_js
on a.application_id = app_js.application_id

    WHERE a.application_date BETWEEN (to_date("2022-01-01"- INTERVAL 10 day)) AND to_date("2022-01-30")
;