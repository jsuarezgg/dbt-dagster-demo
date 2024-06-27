

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
  bronze.twilio_messages
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

-- DBT INCREMENTAL SENTENCE

WHERE `_dt` = to_date("2022-01-01")