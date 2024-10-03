{{
    config(
        materialized='incremental',
        unique_key="id",
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

-- BASE TABLES

WITH co_pii_applications AS (

    SELECT * FROM {{ source('silver_live', 'f_pii_applications_co') }}

),

-- COLOMBIA CELLPHONES

f_origination_events_co_logs_cellphones AS (
    SELECT
        application_id,
        event_name
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
    WHERE event_name IN (
      'CellphoneAlreadyLinkedToADifferentProspectCO',
      'CellphoneAlreadyLinkedToExistingClientCO',
      'ProspectAlreadyLinkedToDifferentCellphoneCO',
      'CellphoneListedInBlackListCO'
    )
),

f_origination_events_co_logs_mails AS (
    SELECT
        application_id,
        event_name
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
    WHERE event_name IN (
      'EmailAlreadyLinkedToExistingClientCO',
      'InvalidEmailCO'
    )
),

f_originations_bnpl_co AS (
    SELECT
        application_id,
        loan_id
    FROM {{ source('silver_live', 'f_originations_bnpl_co') }}
),

co_pii_applications_cellphones AS (

    SELECT
        app.client_id,
        CASE WHEN orig.application_id IS NOT NULL THEN TRUE END AS was_originated,
        from_utc_timestamp(app.application_date, 'America/Bogota') AS application_date,
        app.application_cellphone
    FROM co_pii_applications app
    LEFT JOIN f_originations_bnpl_co orig   ON app.application_id = orig.application_id
    WHERE app.application_cellphone IS NOT NULL
      AND NOT EXISTS (
        SELECT * FROM f_origination_events_co_logs_cellphones events WHERE events.application_id = app.application_id
      )

),

co_pii_applications_cellphones_originated AS (

    SELECT DISTINCT
        a.client_id,
        a.application_cellphone,
        a.application_date,
        b.client_originated
    FROM co_pii_applications_cellphones a
        LEFT JOIN (

            SELECT
                client_id,
                array_contains(array_agg(was_originated), true) AS client_originated
            FROM co_pii_applications_cellphones
            GROUP BY 1

        ) b ON a.client_id = b.client_id

),

co_phones AS (

    SELECT
        client_id,
        application_cellphone
    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY application_cellphone
                ORDER BY client_originated DESC, application_date) AS rn
        FROM co_pii_applications_cellphones_originated

          )
    WHERE rn = 1

),

-- COLOMBIA EMAILS

co_pii_applications_emails AS (
    SELECT
        app.client_id,
        CASE WHEN orig.application_id IS NOT NULL THEN TRUE END AS was_originated,
        from_utc_timestamp(app.application_date, 'America/Bogota') AS application_date,
        LOWER(TRIM(app.application_email)) AS application_email
    FROM co_pii_applications app
    LEFT JOIN f_originations_bnpl_co orig   ON app.application_id = orig.application_id
    WHERE app.application_cellphone IS NOT NULL
      AND NOT EXISTS (
        SELECT * FROM f_origination_events_co_logs_mails events WHERE events.application_id = app.application_id
      )
),

co_pii_applications_emails_originated AS (

    SELECT DISTINCT
        a.client_id,
        a.application_email,
        a.application_date,
        b.client_originated
    FROM co_pii_applications_emails a
        LEFT JOIN (

            SELECT
                client_id,
                array_contains(array_agg(was_originated), true) AS client_originated
            FROM co_pii_applications_emails
            GROUP BY 1

        ) b ON a.client_id = b.client_id

),

co_emails AS (

    SELECT
        client_id,
        application_email
    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY application_email
                ORDER BY client_originated DESC, application_date) AS rn
        FROM co_pii_applications_emails_originated

          )
    WHERE rn = 1

),

communications AS (
  SELECT
    `_dt`,
    to_timestamp(last_event_time) AS last_event_time,
    msg_id AS id,
    'sendgrid' AS source,
    channel,
    from_email AS `from`,
    lower(to_email) AS `to`,
    NULL AS twilio_country_client,
    subject AS message,
    status,
    opens_count AS sendgrid_opens_count,
    clicks_count AS sendgrid_clicks_count,
    NULL AS twilio_price_unit,
    NULL AS twilio_price,
    NULL AS twilio_date_sent,
    NULL AS twilio_date_created,
    NULL AS twilio_direction,
    NULL AS twilio_num_media,
    NULL AS twilio_error_code,
    NULL AS twilio_error_message,
    NULL AS twilio_account_sid,
    NULL AS twilio_num_segments
  FROM
    {{ ref ('f_sendgrid_messages') }}

  UNION ALL

  SELECT
    `_dt`,
    date_updated AS last_event_time,
    sid AS id,
    'twilio' AS source,
    channel,
    `from`,
    `to`,
    CASE
      WHEN direction = 'outbound-api'
      AND left(`to`, 3) = '+57' THEN 'CO'
      WHEN direction = 'inbound'
      AND left(`from`, 3) = '+57' THEN 'CO'
      WHEN direction = 'outbound-api'
      AND left(`to`, 3) = '+55' THEN 'BR'
      WHEN direction = 'inbound'
      AND left(`from`, 3) = '+55' THEN 'BR'
      ELSE 'unknown'
    END AS twilio_country_client,
    body AS message,
    status,
    NULL AS sendgrid_opens_count,
    NULL AS sendgrid_clicks_count,
    price_unit AS twilio_price_unit,
    price AS twilio_price,
    date_sent AS twilio_date_sent,
    date_created AS twilio_date_created,
    direction AS twilio_direction,
    num_media AS twilio_num_media,
    error_code AS twilio_error_code,
    error_message AS twilio_error_message,
    account_sid AS twilio_account_sid,
    num_segments AS twilio_num_segments
  FROM
    {{ ref ('f_twilio_messages') }}
)
SELECT

  cm.`_dt`,
  cm.last_event_time,
  cm.id,
  cm.source,
  cm.channel,
  cm.`from`,
  cm.`to`,
  coalesce(coph.client_id, em.client_id) AS client_id,
  cm.message,
  cm.status,
  cm.sendgrid_opens_count,
  cm.sendgrid_clicks_count,
  cm.twilio_country_client,
  cm.twilio_price_unit,
  cm.twilio_price,
  cm.twilio_date_sent,
  cm.twilio_date_created,
  cm.twilio_direction,
  cm.twilio_num_media,
  cm.twilio_error_code,
  cm.twilio_error_message,
  cm.twilio_account_sid,
  cm.twilio_num_segments
FROM

  communications cm

  LEFT JOIN co_emails em
    ON cm.channel = 'email'
    AND cm.`to` = em.application_email

  LEFT JOIN co_phones coph
    ON cm.twilio_country_client = 'CO'
    AND cm.source = 'twilio'
    AND (replace(cm.`from`, '+57', '') = coph.application_cellphone
    OR replace(cm.`to`, '+57', '') = coph.application_cellphone)

-- DBT INCREMENTAL SENTENCE

{%- if is_incremental() %}

WHERE `_dt` = to_date("{{ var('start_date') }}")

{%- endif %}