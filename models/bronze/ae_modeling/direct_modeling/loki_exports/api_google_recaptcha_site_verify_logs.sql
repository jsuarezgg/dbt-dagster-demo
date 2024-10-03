{{
    config(
        materialized='incremental',
        unique_key='log_request_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- Context I: https://addico.slack.com/archives/C07JC7LS1PW/p1725552960036659?thread_ts=1725306238.416039&cid=C07JC7LS1PW
-- Context II: https://app.asana.com/0/1201607983342231/1208227805142172/f
--addi_prod.raw.api_addi_clients_payments_bancolombia_collect
SELECT
  'api_google_recaptcha_site_verify' AS data_platform_source,
  log.request AS log_request_id,
  log.url AS log_endpoint,
--  NULL AS log_url_id_number,
  log.description AS log_description,
  `timestamp`::TIMESTAMP AS log_timestamp,
--  NULL AS log_body_cellphone_last_digits,
  log.verb AS log_verb,
  log.trace_id AS log_trace_id,
  log.trace AS log_trace,
  log._entry:flowId AS log_entry_flow_id,
--  NULL::STRING AS log_headers_cf_connecting_ip,
--  NULL::STRING AS log_headers_x_forwarded_for,
--  NULL::STRING AS log_headers_user_agent,
  log.status::STRING AS log_status,
  log.body:score::DOUBLE AS log_score,
  log.body:success::BOOLEAN AS log_success --,*
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'api_google_recaptcha_site_verify') }}
{% if is_incremental() %}
WHERE to_date(`timestamp`::TIMESTAMP) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY log.trace_id ORDER BY `timestamp`::TIMESTAMP DESC) = 1