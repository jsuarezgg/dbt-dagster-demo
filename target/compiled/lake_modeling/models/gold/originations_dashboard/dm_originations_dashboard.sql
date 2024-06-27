-- select null from gold.risk_master_table_co limit 1
with rmt_co as ( select
rmt.loan_id
,rmt.application_id
,rmt.addi_pd
,rmt.bureau_pd
,lp.interest_rate
,COALESCE(lp.ally_mdf,rmt.mdf) mdf
from gold.risk_master_table_co rmt
LEFT JOIN silver.f_loan_proposals_co lp ON lp.loan_proposal_id=rmt.loan_id
),

rmt_br as ( select
rmt.loan_id
,rmt.application_id
,rmt.addi_pd
,rmt.bureau_pd
,lp.interest_rate
,COALESCE(lp.ally_mdf,rmt.mdf) mdf
from gold.risk_master_table_br rmt
LEFT JOIN silver.f_loan_proposals_br lp ON lp.loan_proposal_id=rmt.loan_id
),

applications_co (

    SELECT * FROM silver.f_applications_co

),

applications_br (

    SELECT * FROM silver.f_applications_br

),

underwriting_fraud_stage_co (

    SELECT * FROM silver.f_underwriting_fraud_stage_co

),

underwriting_fraud_stage_br (

    SELECT * FROM silver.f_underwriting_fraud_stage_br

),

bnpl_co ( select
application_id
,loan_id
,from_utc_timestamp(origination_date,"America/Bogota") origination_date
,approved_amount
,guarantee_rate
,term
,lbl
from silver.f_originations_bnpl_co
),

bnpl_br ( select
application_id
,loan_id
,approved_amount
,from_utc_timestamp(origination_date,"America/Sao_Paulo") origination_date
,term
,lbl
from silver.f_originations_bnpl_br
),

bnpn_br ( select
application_id
,from_utc_timestamp(origination_date,"America/Sao_Paulo") origination_date
from silver.f_originations_bnpn_br
),

originations_avg_step1_br  as (
select
"BR" as country,
origination_date::date origination_date,
t.application_id,
app.ally_slug,
app.client_type,
app.journey_name,
app.channel,
case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end as amount
from bnpl_br t
left join applications_br app on app.application_id=t.application_id

UNION ALL

select
"BR" as country,
origination_date::date origination_date,
t.application_id,
app.ally_slug,
app.client_type,
app.journey_name,
app.channel,
case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end as amount
from bnpn_br t
left join applications_br app on app.application_id=t.application_id
),



     

    originations_avg_step2_day_null_br as (
    select
    country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_day_Ally_Slug_br as (
    select
    country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_day_Client_Type_br as (
    select
    country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_day_Journey_Name_br as (
    select
    country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_day_Channel_br as (
    select
    country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     



     

    originations_avg_step2_week_null_br as (
    select
    country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_week_Ally_Slug_br as (
    select
    country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_week_Client_Type_br as (
    select
    country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_week_Journey_Name_br as (
    select
    country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_week_Channel_br as (
    select
    country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     



     

    originations_avg_step2_month_null_br as (
    select
    country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_month_Ally_Slug_br as (
    select
    country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_month_Client_Type_br as (
    select
    country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_month_Journey_Name_br as (
    select
    country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_month_Channel_br as (
    select
    country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     



     

    originations_avg_step2_quarter_null_br as (
    select
    country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_quarter_Ally_Slug_br as (
    select
    country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_quarter_Client_Type_br as (
    select
    country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_quarter_Journey_Name_br as (
    select
    country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_quarter_Channel_br as (
    select
    country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     



     

    originations_avg_step2_year_null_br as (
    select
    country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_year_Ally_Slug_br as (
    select
    country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_year_Client_Type_br as (
    select
    country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_year_Journey_Name_br as (
    select
    country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

    originations_avg_step2_year_Channel_br as (
    select
    country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel END as metric,
    avg(amount) originations_avg 
    from originations_avg_step1_br
    group by 1,2,3,4),

     

 



    

    final_table_co_day_null as (
    select
    "CO" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_day_Ally_Slug as (
    select
    "CO" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_day_Client_Type as (
    select
    "CO" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_day_Journey_Name as (
    select
    "CO" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_day_Channel as (
    select
    "CO" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    



    

    final_table_co_week_null as (
    select
    "CO" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_week_Ally_Slug as (
    select
    "CO" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_week_Client_Type as (
    select
    "CO" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_week_Journey_Name as (
    select
    "CO" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_week_Channel as (
    select
    "CO" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    



    

    final_table_co_month_null as (
    select
    "CO" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_month_Ally_Slug as (
    select
    "CO" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_month_Client_Type as (
    select
    "CO" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_month_Journey_Name as (
    select
    "CO" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_month_Channel as (
    select
    "CO" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    



    

    final_table_co_quarter_null as (
    select
    "CO" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_quarter_Ally_Slug as (
    select
    "CO" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_quarter_Client_Type as (
    select
    "CO" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_quarter_Journey_Name as (
    select
    "CO" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_quarter_Channel as (
    select
    "CO" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    



    

    final_table_co_year_null as (
    select
    "CO" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_year_Ally_Slug as (
    select
    "CO" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_year_Client_Type as (
    select
    "CO" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_year_Journey_Name as (
    select
    "CO" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    

    final_table_co_year_Channel as (
    select
    "CO" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    round(avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_avg,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) / count(*) originations_rc_percentage,
    round(SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)) originations_rs,
    max(origination_date) last_origination,
    coalesce(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term),0)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_term,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_apr,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*guarantee_rate) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_mdf,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_addi_pd,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric)/SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_bureau_pd,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%' or 
    (case 
    when app.product is not null then app.product 
     
    when ufs.credit_policy_name= 'addipago_0aprfga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_0fga_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_claro_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_mario_h_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_no_history_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'adelante_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'closing_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'finalization_policy_pago' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_addipago_policy_amoblando' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_closing_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_finalization_policy' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0aprfga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_0fga' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_claro' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_mario_h' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_pago_standard' then 'PAGO_CO'
     
    when ufs.credit_policy_name= 'rc_rejection_policy' then 'PAGO_CO'
    
    else 'FINANCIA_CO' end)='PAGO_CO'
    ) as numeric) / count(*) addipago_percentage,
    cast(COUNT(*) filter (where t.lbl = true) as numeric) / count(*) lbl_percentage,
    cast(COUNT(*) filter (where guarantee_rate>0) as numeric) / count(*) fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_co t
    left join applications_co app on app.application_id=t.application_id
    left join rmt_co rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_co ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5
    ),

    



final_table_co as (



    

        select * from final_table_co_day_null
        UNION ALL
        
    

        select * from final_table_co_day_Ally_Slug
        UNION ALL
        
    

        select * from final_table_co_day_Client_Type
        UNION ALL
        
    

        select * from final_table_co_day_Journey_Name
        UNION ALL
        
    

        select * from final_table_co_day_Channel
    
UNION ALL
   


    

        select * from final_table_co_week_null
        UNION ALL
        
    

        select * from final_table_co_week_Ally_Slug
        UNION ALL
        
    

        select * from final_table_co_week_Client_Type
        UNION ALL
        
    

        select * from final_table_co_week_Journey_Name
        UNION ALL
        
    

        select * from final_table_co_week_Channel
    
UNION ALL
   


    

        select * from final_table_co_month_null
        UNION ALL
        
    

        select * from final_table_co_month_Ally_Slug
        UNION ALL
        
    

        select * from final_table_co_month_Client_Type
        UNION ALL
        
    

        select * from final_table_co_month_Journey_Name
        UNION ALL
        
    

        select * from final_table_co_month_Channel
    
UNION ALL
   


    

        select * from final_table_co_quarter_null
        UNION ALL
        
    

        select * from final_table_co_quarter_Ally_Slug
        UNION ALL
        
    

        select * from final_table_co_quarter_Client_Type
        UNION ALL
        
    

        select * from final_table_co_quarter_Journey_Name
        UNION ALL
        
    

        select * from final_table_co_quarter_Channel
    
UNION ALL
   


    

        select * from final_table_co_year_null
        UNION ALL
        
    

        select * from final_table_co_year_Ally_Slug
        UNION ALL
        
    

        select * from final_table_co_year_Client_Type
        UNION ALL
        
    

        select * from final_table_co_year_Journey_Name
        UNION ALL
        
    

        select * from final_table_co_year_Channel
       


),



    

    final_table_br_day_null as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_day_null_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_day_Ally_Slug as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_day_Ally_Slug_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_day_Client_Type as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_day_Client_Type_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_day_Journey_Name as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_day_Journey_Name_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_day_Channel as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'day' period_type,
    CASE when 'day'='day' THEN origination_date::date
    ELSE date_trunc('day',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_day_Channel_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    



    

    final_table_br_week_null as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_week_null_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_week_Ally_Slug as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_week_Ally_Slug_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_week_Client_Type as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_week_Client_Type_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_week_Journey_Name as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_week_Journey_Name_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_week_Channel as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'week' period_type,
    CASE when 'week'='day' THEN origination_date::date
    ELSE date_trunc('week',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_week_Channel_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    



    

    final_table_br_month_null as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_month_null_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_month_Ally_Slug as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_month_Ally_Slug_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_month_Client_Type as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_month_Client_Type_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_month_Journey_Name as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_month_Journey_Name_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_month_Channel as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'month' period_type,
    CASE when 'month'='day' THEN origination_date::date
    ELSE date_trunc('month',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_month_Channel_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    



    

    final_table_br_quarter_null as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_quarter_null_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_quarter_Ally_Slug as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_quarter_Ally_Slug_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_quarter_Client_Type as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_quarter_Client_Type_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_quarter_Journey_Name as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_quarter_Journey_Name_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_quarter_Channel as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'quarter' period_type,
    CASE when 'quarter'='day' THEN origination_date::date
    ELSE date_trunc('quarter',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_quarter_Channel_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    



    

    final_table_br_year_null as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'null'='null' THEN "ALL"
    ELSE 'null' 
    END as breakdown_metric,
    CASE WHEN 'null'='null' THEN "ALL" 
    ELSE null 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_year_null_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_year_Ally_Slug as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL"
    ELSE 'Ally_Slug' 
    END as breakdown_metric,
    CASE WHEN 'Ally_Slug'='null' THEN "ALL" 
    ELSE Ally_Slug 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_year_Ally_Slug_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_year_Client_Type as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Client_Type'='null' THEN "ALL"
    ELSE 'Client_Type' 
    END as breakdown_metric,
    CASE WHEN 'Client_Type'='null' THEN "ALL" 
    ELSE Client_Type 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_year_Client_Type_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_year_Journey_Name as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Journey_Name'='null' THEN "ALL"
    ELSE 'Journey_Name' 
    END as breakdown_metric,
    CASE WHEN 'Journey_Name'='null' THEN "ALL" 
    ELSE Journey_Name 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_year_Journey_Name_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    

    final_table_br_year_Channel as (
    select
    q1.country,
    q1.period_type,
    q1.date_value,
    breakdown_metric,
    breakdown_value,
    -- CASE WHEN =1 THEN "Brazilian_Real"
    -- WHEN =5.18 THEN "Dollar"
    -- ELSE null
    -- END as Money_Type,
    round(q2.originations_avg) originations_avg,
    -- /replace(,'_','.')) originations_avg,
    sum(interest_rate) interest_rate,
    sum(term) term,
    sum(originations) originations,
    sum(rc)/sum(originations) originations_rc_percentage,
    round(sum(originations_rs)) originations_rs,
    max(last_origination) last_origination,
    sum(weighted_term_numerator)/sum(weighted_common_denominator) weighted_term,
    sum(weighted_apr_numerator)/sum(weighted_common_denominator) weighted_apr,
    sum(weighted_fga_rate) weighted_fga_rate,
    sum(weighted_mdf_numerator)/sum(weighted_common_denominator) weighted_mdf,
    sum(weighted_addi_pd_numerator)/sum(weighted_common_denominator) weighted_addi_pd,
    sum(weighted_bureau_pd_numerator)/sum(weighted_common_denominator) weighted_bureau_pd,
    sum(addipago_percentage_numerator)/sum(originations) addipago_percentage,
    sum(lbl_percentage) lbl_percentage,
    sum(fga_percentage) fga_percentage,
    sum(expected_losses) expected_losses
    from
    (select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    sum(rmt.interest_rate) interest_rate,
    sum(t.term) term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*t.term) as numeric) weighted_term_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.interest_rate) as numeric) weighted_apr_numerator,
    null as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    cast(COUNT(*) filter (where t.lbl = true) as numeric)/count(*) lbl_percentage,
    null as fga_percentage,
    sum(case 
    when t.term=3 then 0.538*rmt.addi_pd*t.approved_amount
    when t.term=6 then 0.712*rmt.addi_pd*t.approved_amount 
    when t.term=9 then 0.803*rmt.addi_pd*t.approved_amount
    when t.term=12 then 0.864*rmt.addi_pd*t.approved_amount 
    when t.term=18 then 1.071*rmt.addi_pd*t.approved_amount
    when t.term=24 then 1.194*rmt.addi_pd*t.approved_amount
    else null end) expected_losses
    from
    bnpl_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.loan_id=t.loan_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5

    UNION ALL

    select
    "BR" as country,
    'year' period_type,
    CASE when 'year'='day' THEN origination_date::date
    ELSE date_trunc('year',origination_date::date) 
    END as date_value,
    CASE WHEN 'Channel'='null' THEN "ALL"
    ELSE 'Channel' 
    END as breakdown_metric,
    CASE WHEN 'Channel'='null' THEN "ALL" 
    ELSE Channel 
    END as breakdown_value,
    0 as interest_rate,
    0 as term,
    count(*) originations,
    cast(COUNT(*) filter (where app.client_type = 'CLIENT') as numeric) rc,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_rs,
    avg(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end) originations_avg,
    max(origination_date) last_origination,
    0 as weighted_term,
    0 as weighted_apr,
    0 as weighted_fga_rate,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.mdf) as numeric) weighted_mdf_numerator,
    SUM(case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)
    weighted_common_denominator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.addi_pd) as numeric) weighted_addi_pd_numerator,
    cast(SUM((case when app.requested_amount_without_discount>0 then app.requested_amount_without_discount else app.requested_amount end)*rmt.bureau_pd) as numeric) weighted_bureau_pd_numerator,
    cast(COUNT(*) filter (where lower(app.journey_name) like '%pago%') as numeric) addipago_percentage_numerator,
    0 as lbl_percentage,
    0 as fga_percentage,
    0 as expected_losses
    from
    bnpn_br t
    left join applications_br app on app.application_id=t.application_id
    left join rmt_br rmt on rmt.application_id=t.application_id
    left join underwriting_fraud_stage_br ufs on ufs.application_id=t.application_id
    group by 1,2,3,4,5) q1
    left join originations_avg_step2_year_Channel_br q2 
    on q1.country=q2.country and q1.date_value=q2.date_value and q1.breakdown_metric=q2.metric
    and q1.period_type=q2.period_type
    group by 1,2,3,4,5,6
    ),

    



final_table_br as (



    

        select * from final_table_br_day_null
        UNION ALL
        
    

        select * from final_table_br_day_Ally_Slug
        UNION ALL
        
    

        select * from final_table_br_day_Client_Type
        UNION ALL
        
    

        select * from final_table_br_day_Journey_Name
        UNION ALL
        
    

        select * from final_table_br_day_Channel
    
UNION ALL



    

        select * from final_table_br_week_null
        UNION ALL
        
    

        select * from final_table_br_week_Ally_Slug
        UNION ALL
        
    

        select * from final_table_br_week_Client_Type
        UNION ALL
        
    

        select * from final_table_br_week_Journey_Name
        UNION ALL
        
    

        select * from final_table_br_week_Channel
    
UNION ALL



    

        select * from final_table_br_month_null
        UNION ALL
        
    

        select * from final_table_br_month_Ally_Slug
        UNION ALL
        
    

        select * from final_table_br_month_Client_Type
        UNION ALL
        
    

        select * from final_table_br_month_Journey_Name
        UNION ALL
        
    

        select * from final_table_br_month_Channel
    
UNION ALL



    

        select * from final_table_br_quarter_null
        UNION ALL
        
    

        select * from final_table_br_quarter_Ally_Slug
        UNION ALL
        
    

        select * from final_table_br_quarter_Client_Type
        UNION ALL
        
    

        select * from final_table_br_quarter_Journey_Name
        UNION ALL
        
    

        select * from final_table_br_quarter_Channel
    
UNION ALL



    

        select * from final_table_br_year_null
        UNION ALL
        
    

        select * from final_table_br_year_Ally_Slug
        UNION ALL
        
    

        select * from final_table_br_year_Client_Type
        UNION ALL
        
    

        select * from final_table_br_year_Journey_Name
        UNION ALL
        
    

        select * from final_table_br_year_Channel
    


),

final_table as (
select * from final_table_br
union ALL
select * from final_table_co
),

final_unified_table as (
select country, period_type, date_value, breakdown_metric,breakdown_value,last_origination,"Total_originations" as metric,cast(originations as double) as value from final_table
UNION ALL
select country, period_type, date_value, breakdown_metric,breakdown_value,last_origination,"Originations_rc_percentage" as metric,round(cast(originations_rc_percentage as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value, breakdown_metric,breakdown_value,last_origination,"Total_originations_amount" as metric,round(cast(originations_rs as double),2) as value from final_table
UNION ALL
select country, period_type, date_value, breakdown_metric,breakdown_value,last_origination,"Avg_ticket_amount" as metric,round(cast(originations_avg as double),2) as value from final_table
UNION ALL
select country, period_type, date_value, breakdown_metric,breakdown_value,last_origination,"Weighted_term_percentage" as metric,round(cast(weighted_term as double),2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"Weighted_APR_percentage" as metric,round(cast(weighted_apr as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"Weighted_FGA_rate_percentage" as metric,round(cast(weighted_fga_rate as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"Weighted_ADDI_PD_percentage" as metric,round(cast(weighted_addi_pd as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"Weighted_Bureau_PD_percentage" as metric,round(cast(weighted_bureau_pd as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"Addipago_originations_percentage" as metric,round(cast(addipago_percentage as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"LBL_originations_percentage" as metric,round(cast(lbl_percentage as double)*100,2) as value from final_table
UNION ALL
select country, period_type, date_value,  breakdown_metric,breakdown_value,last_origination,"Weighted_MDF_percentage" as metric,round(cast(weighted_mdf as double)*100,2) as value from final_table
order by country,period_type,date_value
)

select *
,NOW() AS ingested_at
 from final_unified_table