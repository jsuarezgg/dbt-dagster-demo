{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}



WITH amplitude_calculations as (
SELECT
CASE WHEN event_type='NOTIFICATIONS_PUSH_NOTIFICATION_TAPED' THEN 'PUSH'
ELSE 'BANNER' END AS channel,
event_properties:campaignId AS campaign_id,
count(distinct(user_id)) AS amplitude_click
FROM {{ ref('f_amplitude_addi_funnel_project') }}
WHERE event_type in ('NOTIFICATIONS_PUSH_NOTIFICATION_TAPED','HOME_PROMOTED_BANNER_TAPPED') 
GROUP BY 1,2
),

shopping_calculations as (
SELECT
CASE WHEN upper(si.screen)='DEALS' THEN 'DEALS'
ELSE si.channel END AS channel,
si.campaign_id,
CASE WHEN max(si.ally_slug) ilike '%addi%' THEN true
ELSE false END AS general_addi_flag,
COUNT(distinct si.shopping_intent_id) AS shopping_intents,
COUNT(distinct ta.application_id) AS attributable_loans,
SUM(o.gmv) AS attributable_gmv
FROM {{ source('silver_live', 'f_marketplace_shopping_intents_co') }} si
LEFT JOIN {{ source('silver_live', 'f_marketplace_transaction_attributable_co') }} ta ON si.shopping_intent_id=ta.shopping_intent_id
LEFT JOIN {{ ref('dm_originations') }} o ON ta.application_id=o.application_id
WHERE si.channel not in ('WEB','MOBILE_APP','APP_BANNER','PUSH_NOTIFICATION','') or (si.channel in ('MOBILE_APP') and upper(si.screen)='DEALS')
GROUP BY 1,2
),

communications_calculations as (
SELECT
cc.campaign_name,
cc.channel,
CASE WHEN cc.campaign_name is null or cc.campaign_name='' THEN null
ELSE min(dcc.created_at)::date END AS campaign_start_date,
COUNT(*) FILTER (WHERE cc.communication_status='read') AS communication_read,
COUNT(*) FILTER (WHERE cc.communication_status='open') AS communication_open,
COUNT(*) FILTER (WHERE cc.communication_status='delivered') AS communication_delivered,
COUNT(*) FILTER (WHERE cc.communication_status='bounce') AS communication_bounce,
COUNT(*) FILTER (WHERE cc.communication_status='queued') AS communication_queued,
COUNT(*) FILTER (WHERE cc.communication_status='dropped') AS communication_dropped,
COUNT(*) FILTER (WHERE cc.communication_status='click') AS communication_click,
COUNT(*) FILTER (WHERE cc.communication_status='undelivered') AS communication_undelivered,
COUNT(*) FILTER (WHERE cc.communication_status='processed') AS communication_processed,
COUNT(*) FILTER (WHERE cc.communication_status='deferred') AS communication_deferred,
COUNT(*) FILTER (WHERE cc.communication_status='spamreport') AS communication_spamreport,
COUNT(*) FILTER (WHERE cc.communication_status ilike '%FAILED%' ) AS communication_failed,
COUNT(*) FILTER (WHERE cc.communication_status='sent') AS communication_sent,
COUNT(*) FILTER (WHERE cc.communication_status in ('read','sent','spamreport','deferred','processed','undelivered','click'
,'dropped','queued','bounce','delivered','open') or communication_status ilike '%FAILED%') AS total_communications
FROM {{ ref('f_communications_communication_co') }} cc
LEFT JOIN {{ ref('d_communications_campaign_co') }} dcc ON cc.campaign_name=dcc.name
GROUP BY 1,2
),

shopping_and_communications_calculations as (
SELECT
CASE WHEN campaign_id='' THEN cc.campaign_name
ELSE COALESCE(sc.campaign_id,cc.campaign_name) END AS campaign_id,
COALESCE(sc.channel,cc.channel) AS channel,
sc.general_addi_flag,
CASE WHEN cc.campaign_start_date is not null THEN cc.campaign_start_date
WHEN sc.campaign_id is not null THEN make_date(LEFT(sc.campaign_id,4),substring(sc.campaign_id FROM 5 FOR 2),substring(sc.campaign_id FROM 7 FOR 2))
WHEN sc.campaign_id is null THEN null
END AS campaign_start_date,
max(sc.shopping_intents) AS shopping_intents,
max(sc.attributable_loans) AS attributable_loans,
max(sc.attributable_gmv) AS attributable_gmv,
sum(cc.communication_read) AS communication_read,
sum(cc.communication_open) AS communication_open,
sum(cc.communication_delivered) AS communication_delivered,
sum(cc.communication_bounce) AS communication_bounce,
sum(cc.communication_queued) AS communication_queued,
sum(cc.communication_dropped) AS communication_dropped,
sum(cc.communication_click) AS communication_click,
sum(cc.communication_undelivered) AS communication_undelivered,
sum(cc.communication_processed) AS communication_processed,
sum(cc.communication_deferred) AS communication_deferred,
sum(cc.communication_spamreport) AS communication_spamreport,
sum(cc.communication_failed) AS communication_failed,
sum(cc.communication_sent) AS communication_sent,
sum(cc.total_communications) AS total_communications
FROM shopping_calculations sc
FULL JOIN communications_calculations cc ON cc.campaign_name like concat(sc.campaign_id,'%') and cc.channel=sc.channel
GROUP BY 1,2,3,4
),

final_table as (
SELECT 
COALESCE(scc.channel,ac.channel) AS channel,
COALESCE(scc.campaign_id,ac.campaign_id) AS campaign_id,
scc.campaign_start_date,
scc.general_addi_flag,
scc.shopping_intents,
scc.attributable_loans,
scc.attributable_gmv,
scc.communication_read,
scc.communication_open,
scc.communication_delivered,
scc.communication_bounce,
scc.communication_queued,
scc.communication_dropped,
scc.communication_click,
ac.amplitude_click,
scc.communication_undelivered,
scc.communication_processed,
scc.communication_deferred,
scc.communication_spamreport,
scc.communication_failed,
scc.communication_sent,
scc.total_communications+COALESCE(ac.amplitude_click,0) AS total_communications,
try_divide(communication_open+communication_read+communication_click,total_communications) AS open_rate,
try_divide(communication_click,total_communications) AS click_through_rate ,
try_divide(communication_bounce,total_communications) AS bounce_rate,
try_divide(communication_spamreport,total_communications) AS unsuscribe_rate,
try_divide(communication_delivered+communication_open+communication_read+communication_click,total_communications) AS delivery_rate
FROM shopping_and_communications_calculations scc
FULL JOIN amplitude_calculations ac ON ac.campaign_id=scc.campaign_id and ac.channel=scc.channel
WHERE COALESCE(scc.channel,ac.channel) is not null or COALESCE(scc.channel,ac.channel) is not null)


SELECT *
FROM final_table