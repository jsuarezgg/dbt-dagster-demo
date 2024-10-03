{{
    config(
        materialized='incremental',
        unique_key='log_trace_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- Context I: https://addico.slack.com/archives/C07JC7LS1PW/p1725552960036659?thread_ts=1725306238.416039&cid=C07JC7LS1PW
-- Context II: https://app.asana.com/0/1201607983342231/1208227805142172/f
WITH api_addi_clients_payments_bancolombia_collect_logs AS (
SELECT
  data_platform_source,
  log_request_id,
  log_endpoint,
  log_url_id_number,
  log_description,
  log_timestamp,
  log_body_cellphone_last_digits,
  log_verb,
  log_trace_id,
  log_trace,
  log_entry_flow_id,
  log_headers_cf_connecting_ip,
  log_headers_x_forwarded_for,
  log_headers_user_agent,
  NULL::STRING AS log_status,
  NULL::DOUBLE AS log_score,
  NULL::BOOLEAN AS log_success --,*
FROM {{ ref('api_addi_clients_payments_bancolombia_collect_logs') }}
WHERE log_trace_id IS NOT NULL
{% if is_incremental() %}
AND to_date(log_timestamp) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
{% endif %}
)
,
api_google_recaptcha_site_verify_logs AS (
SELECT
  data_platform_source,
  log_request_id,
  log_endpoint,
  NULL AS log_url_id_number,
  log_description,
  log_timestamp,
  NULL AS log_body_cellphone_last_digits,
  log_verb,
  log_trace_id,
  log_trace,
  log_entry_flow_id,
  NULL::STRING AS log_headers_cf_connecting_ip,
  NULL::STRING AS log_headers_x_forwarded_for,
  NULL::STRING AS log_headers_user_agent,
  log_status,
  log_score,
  log_success --,*
FROM {{ ref('api_google_recaptcha_site_verify_logs') }}
WHERE log_trace_id IS NOT NULL
{% if is_incremental() %}
AND to_date(log_timestamp) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
{% endif %}
)

SELECT
  CASE
    WHEN a.log_trace_id IS NOT NULL AND g.log_trace_id IS NOT NULL THEN 'both'
    WHEN a.log_trace_id IS NOT NULL THEN 'clients_payments_bancolombia_collect'
    WHEN g.log_trace_id IS NOT NULL THEN 'google_recaptcha_site_verify'
    ELSE '__UNKNOWN__'
  END AS log_sources,
  COALESCE(a.log_trace_id, g.log_trace_id) log_trace_id,
  a.log_request_id AS clients_payments_request_id,
  a.log_timestamp AS clients_payments_timestamp,
  g.log_request_id AS google_recaptcha_request_id,
  g.log_timestamp AS google_recaptcha_timestamp,
  a.log_endpoint AS clients_payments_endpoint,
  g.log_endpoint AS google_recaptcha_endpoint,
  a.log_verb AS clients_payments_verb,
  g.log_verb AS google_recaptcha_verb,
  a.log_url_id_number AS clients_payments_id_number,
  a.log_headers_cf_connecting_ip AS clients_payments_cf_connecting_ip,
  a.log_headers_x_forwarded_for AS clients_payments_x_forwarded_for,
  a.log_headers_user_agent AS clients_payments_user_agent,
  g.log_status AS google_recaptcha_status,
  g.log_score AS google_recaptcha_score,
  g.log_success AS google_recaptcha_is_success
FROM api_addi_clients_payments_bancolombia_collect_logs AS a
FULL OUTER JOIN api_google_recaptcha_site_verify_logs AS g ON a.log_trace_id = g.log_trace_id

