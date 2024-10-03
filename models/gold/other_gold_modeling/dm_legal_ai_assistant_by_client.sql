{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH pii_application as (
  SELECT 
  client_id,
  application_cellphone,
  application_email
  FROM {{ source('silver_live', 'f_pii_applications_co') }} 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY application_date::TIMESTAMP DESC) = 1
),

personal_info (
SELECT ppd.id_number,
ppd.id_type,
ppd.client_id,
COALESCE(pii.application_cellphone,comm.phone_number) as application_cellphone,
COALESCE(pii.application_email,comm.email) as application_email,
ppd.last_name,
ppd.full_name,
ppd.document_expedition_city,
ppd.document_expedition_date
FROM {{ source('silver_live', 'd_prospect_personal_data_co') }} ppd 
LEFT JOIN {{ ref('d_communications_clients_co') }} comm ON ppd.client_id=comm.client_id
LEFT JOIN pii_application pii ON pii.client_id=ppd.client_id
),


privacy_policy_info as (
SELECT
client_id,
ocurred_on as privacy_policy_accepted_date,
  NAMED_STRUCT(
    'event_id', event_id,
      'ocurred_on', ocurred_on,
      'privacy',
          NAMED_STRUCT(
            "isAccepted", custom_is_privacy_policy_accepted
    )
) AS privacy_policy_detail_json,
pps.application_id as privacy_policy_accepted_application_id
FROM {{ ref('f_privacy_policy_stage_co_logs') }} pps
WHERE custom_is_privacy_policy_accepted is true
QUALIFY ROW_NUMBER() OVER (partition by client_id order by ocurred_on asc) = 1
),

message_info as (
SELECT 
substring(to,4) as cellphone,
COLLECT_LIST(NAMED_STRUCT('from',from,
'to',to,
'id',id,
'twilio_date_sent',twilio_date_sent,
'message',message,
'status',status)
) as message_info_detailed_json
FROM {{ ref('dm_communications') }}
WHERE message ILIKE '%centrales%'
AND message ILIKE '%riesgo%'
AND message NOT ILIKE '%investig%'
AND message NOT ILIKE '%calificar el servicio%'
AND channel = 'sms'
AND status in ('delivered','sent','received')
GROUP BY to
),

email_info as (
  SELECT
  recipient,
  COLLECT_LIST(NAMED_STRUCT(
  'sent_on',created_at,
  'communication_status',communication_status,
  'email_message',message)) as email_info_detailed_json
FROM {{ ref('f_communications_communication_co') }}
WHERE channel = 'EMAIL'
AND communication_source = 'CLIENT_MANAGEMENT_DELINQUENT_REPORT'
GROUP BY recipient
)

SELECT ppd.id_number,
ppd.id_type,
ppd.client_id,
ppd.application_cellphone,
ppd.application_email,
ppd.last_name,
ppd.full_name,
ppd.document_expedition_city,
ppd.document_expedition_date,
privacy_policy_accepted_date,
privacy_policy_detail_json,
privacy_policy_accepted_application_id,
message_info_detailed_json,
email_info_detailed_json
FROM personal_info ppd
LEFT JOIN privacy_policy_info ppi ON ppd.client_id=ppi.client_id
LEFT JOIN message_info mi ON mi.cellphone=ppd.application_cellphone
LEFT JOIN email_info ei ON lower(ei.recipient)=lower(ppd.application_email) 