

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
  bronze.sendgrid_messages
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

-- DBT INCREMENTAL SENTENCE

WHERE `_dt` = to_date("2022-01-01")