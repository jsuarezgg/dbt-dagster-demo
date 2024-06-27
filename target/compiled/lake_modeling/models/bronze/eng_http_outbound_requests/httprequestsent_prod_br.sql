

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
    json_obj.request.thirdParty AS third_party,
    json_obj.request.serviceName AS service_name,
    json_obj.request.method AS method,
    json_obj.request.url AS url,
    json_obj.request.headers AS headers,
    json_obj.request.body AS body,
    json_obj.request.formAttributes AS form_attributes
FROM raw_http_outbound_requests.httprequestsent_br


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
