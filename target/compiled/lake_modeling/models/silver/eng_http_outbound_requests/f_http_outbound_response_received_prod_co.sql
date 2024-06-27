

SELECT
    event_name_original,
    event_name,
    event_id,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    trace_id,
    client_id,
    third_party,
    service_name,
    method,
    url,
    headers,
    body,
    status_code,
    duration
FROM bronze.httpresponsereceived_prod_co


    WHERE ocurred_on_date BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND ocurred_on BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
