

SELECT
  msg_id,
  status,
  subject,
  to_email,
  from_email,
  opens_count,
  clicks_count,
  last_event_time,
  to_date(last_event_time) AS `_dt`
FROM
    bronze.sendgrid_messages

-- DBT INCREMENTAL SENTENCE