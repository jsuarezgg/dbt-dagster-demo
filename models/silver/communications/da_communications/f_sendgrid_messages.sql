{{
    config(
        materialized='incremental',
        unique_key="msg_id",
        incremental_strategy='append',
        full_refresh = false,
        partition_by=['_dt'],
        pre_hook=[
            'DELETE FROM {{ this }} WHERE `_dt` = to_date("{{ var("start_date") }}")',
        ],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH sendgrid_base AS (

SELECT
  msg_id,
  'email' AS channel,
  status,
  subject,
  to_email,
  from_email,
  opens_count,
  clicks_count,
  to_timestamp(last_event_time) as last_event_time,
  to_date(last_event_time) AS `_dt`
FROM
  {{ source('bronze', 'sendgrid_messages') }}
)

SELECT
  msg_id,
  channel,
  status,
  subject,
  to_email,
  from_email,
  opens_count,
  clicks_count,
  last_event_time,
  `_dt` AS `_dt`

FROM
  sendgrid_base

{%- if is_incremental() %}

-- DBT INCREMENTAL SENTENCE

WHERE `_dt` = to_date("{{ var('start_date') }}")

{%- endif %}
