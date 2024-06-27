

SELECT
    event_type AS event_name_original,
    reverse(split(event_type,"\\."))[0] AS event_name,
    event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    to_date(ocurred_on) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    json_obj.metadata.context.traceId AS trace_id,
    json_obj.metadata.context.clientId AS client_id,
    json_obj.response.thirdParty AS third_party,
    json_obj.response.serviceName AS service_name,
    json_obj.response.method AS method,
    json_obj.response.url AS url,
    json_obj.response.headers AS headers,
    json_obj.response.body AS body,
    json_obj.response.statusCode AS status_code,
    json_obj.response.duration AS duration
FROM raw_http_outbound_requests.httpresponsereceived_co


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
