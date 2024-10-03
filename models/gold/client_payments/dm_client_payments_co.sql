{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH regular_payments AS (
    SELECT
        from_utc_timestamp(fp.payment_date, 'America/Bogota')::TIMESTAMP AS payment_date,
        from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date AS processing_date,
        fp.payment_id,
        fp.payment_method AS method,
        fp.paid_amount AS amount,
        fp.client_id,
        from_utc_timestamp(fp.payment_date, 'America/Bogota')::date AS payment_date_dt,
        fp.is_annulled AS annulled,
        fp.annulment_reason AS annullment_reason,
        pp.id_number,
        COALESCE(cp.payment_ownership, "ADDI") AS payment_ownership,
        fp.last_event_name_processed AS stage
    FROM {{ source("silver_live", "f_fincore_payments_co") }} fp
    LEFT JOIN {{ source("silver_live", "d_prospect_personal_data_co") }} pp ON fp.client_id = pp.client_id
    LEFT JOIN {{ ref("f_client_payments_client_payments_co") }} cp ON fp.payment_id = cp.payment_id
    WHERE fp.last_event_name_processed = 'ClientPaymentReceivedV3'
    AND fp.is_annulled = false
    -- If first day of the month, consume all previous month
    -- Otherwise, consume from the first day of the month until the previous day of execution
    AND (
        (
            date_part('day', from_utc_timestamp(now(), 'America/Bogota')) = 1
            AND from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date >= date_trunc('month', from_utc_timestamp(now(), 'America/Bogota') - interval '1 month')::date
            AND from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date < date_trunc('month', from_utc_timestamp(now(), 'America/Bogota'))::date
        )
        OR
        (
            date_part('day', from_utc_timestamp(now(), 'America/Bogota')) > 1
            AND from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date >= date_trunc('month', from_utc_timestamp(now(), 'America/Bogota')::date)
            AND from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date < from_utc_timestamp(now(), 'America/Bogota')::date
        )
    )
)

SELECT * FROM regular_payments
ORDER BY processing_date, payment_date
