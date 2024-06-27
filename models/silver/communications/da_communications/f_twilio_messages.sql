{{
    config(
        materialized='incremental',
        unique_key="sid",
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

WITH twilio_base AS (

SELECT
  sid,
  replace(`from`, 'whatsapp:', '') AS `from`,
  replace(`to`, 'whatsapp:', '') AS `to`,
  CASE
  	WHEN `from` IS NULL THEN NULL
  	WHEN `from` LIKE("whatsapp%") THEN 'whatsapp'
  	ELSE 'sms'
  END AS channel,
  body,
  price_unit,
  price,
  status,
  date_sent,
  date_created,
  date_updated,
  direction,
  num_media,
  error_code,
  error_message,
  account_sid,
  num_segments,
  `_day`,
  `_year`,
  `_month`,
  to_date(`_dt`) AS `_dt`
FROM
  {{ source('bronze', 'twilio_messages') }}
)

SELECT 
  sid,
  `from`,
  `to`,
  channel,
  body,
  price_unit,
  price,
  status,
  date_sent,
  date_created,
  date_updated,
  direction,
  num_media,
  error_code,
  error_message,
  account_sid,
  num_segments,
  `_day`,
  `_year`,
  `_month`,
  `_dt`

FROM twilio_base

{%- if is_incremental() %}

-- DBT INCREMENTAL SENTENCE

WHERE `_dt` = to_date("{{ var('start_date') }}")

{%- endif %}
