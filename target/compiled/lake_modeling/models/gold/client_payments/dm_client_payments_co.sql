

WITH 

f_fincore_payments_co AS (

    SELECT * FROM silver.f_fincore_payments_co
    WHERE last_event_name_processed = 'ClientPaymentReceivedV3'
    AND is_annulled = false

),

dm_daily_client_payments_co AS (

    SELECT * FROM gold.dm_daily_client_payments_co

),

d_prospect_personal_data_co AS (

    SELECT * FROM silver.d_prospect_personal_data_co

),

f_client_payments_client_payments_co AS (

    SELECT * FROM silver.f_client_payments_client_payments_co

),

fincore_payments_count AS (

    SELECT
        from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date AS processing_date,
        COUNT(*) AS payments_count
    FROM f_fincore_payments_co fp
    WHERE from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date >= (SELECT MIN(snapshot_date) FROM dm_daily_client_payments_co)
    GROUP BY 1

),

snapshot_count AS (

    SELECT
        snapshot_date,
        COUNT(*) AS payments_count
    FROM dm_daily_client_payments_co
    GROUP BY 1

),

days_with_differences AS (

    SELECT
        fpc.processing_date
    FROM fincore_payments_count fpc
    LEFT JOIN snapshot_count sc ON fpc.processing_date = sc.snapshot_date
    WHERE fpc.payments_count <> sc.payments_count

),

regular_payments AS (

    SELECT
        payment_date,
        processing_date,
        payment_id,
        method,
        amount,
        client_id,
        payment_date_dt,
        annulled,
        annullment_reason,
        id_number,
        payment_ownership,
        stage
    FROM dm_daily_client_payments_co
    WHERE snapshot_date = date_sub(current_date(), 1)

),

payments_missing AS (

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
    FROM f_fincore_payments_co fp
    LEFT JOIN d_prospect_personal_data_co pp ON fp.client_id = pp.client_id
    LEFT JOIN f_client_payments_client_payments_co cp ON fp.payment_id = cp.payment_id
    WHERE from_utc_timestamp(fp.last_event_ocurred_on_processed, 'America/Bogota')::date IN (SELECT * FROM days_with_differences)
    AND fp.payment_id NOT IN (SELECT payment_id FROM dm_daily_client_payments_co)

)

SELECT * FROM regular_payments

UNION

SELECT * FROM payments_missing