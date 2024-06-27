{% macro return_config_br_f_kyc_iovation_v1v2_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_iovation_v1v2_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-22 18:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_iovation_v1v2_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "iovationobtained": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "custom_kyc_event_version",
                "iovation_blackBox",
                "iovation_data_details_device_alias",
                "iovation_data_details_device_blackboxMetadata_age",
                "iovation_data_details_device_blackboxMetadata_timestamp",
                "iovation_data_details_device_browser_configuredLanguage",
                "iovation_data_details_device_browser_cookiesEnabled",
                "iovation_data_details_device_browser_language",
                "iovation_data_details_device_browser_timezone",
                "iovation_data_details_device_browser_type",
                "iovation_data_details_device_browser_version",
                "iovation_data_details_device_firstSeen",
                "iovation_data_details_device_isNew",
                "iovation_data_details_device_os",
                "iovation_data_details_device_screen",
                "iovation_data_details_device_type",
                "iovation_data_details_realIp_address",
                "iovation_data_details_realIp_ipLocation_city",
                "iovation_data_details_realIp_ipLocation_country",
                "iovation_data_details_realIp_ipLocation_countryCode",
                "iovation_data_details_realIp_ipLocation_latitude",
                "iovation_data_details_realIp_ipLocation_longitude",
                "iovation_data_details_realIp_ipLocation_region",
                "iovation_data_details_realIp_isp",
                "iovation_data_details_realIp_parentOrganization",
                "iovation_data_details_realIp_source",
                "iovation_data_details_ruleResults_rulesMatched",
                "iovation_data_details_ruleResults_score",
                "iovation_data_details_statedIp_address",
                "iovation_data_details_statedIp_ipLocation_city",
                "iovation_data_details_statedIp_ipLocation_country",
                "iovation_data_details_statedIp_ipLocation_countryCode",
                "iovation_data_details_statedIp_ipLocation_latitude",
                "iovation_data_details_statedIp_ipLocation_longitude",
                "iovation_data_details_statedIp_ipLocation_region",
                "iovation_data_details_statedIp_isp",
                "iovation_data_details_statedIp_parentOrganization",
                "iovation_data_details_statedIp_source",
                "iovation_data_id",
                "iovation_data_reason",
                "iovation_data_result",
                "iovation_data_trackingNumber",
                "iovation_requestedAt",
                "metadata_context_traceId",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "iovationobtained_v2": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "custom_kyc_event_version",
                "iovation_blackBox",
                "iovation_data_details_device_alias",
                "iovation_data_details_device_blackboxMetadata_age",
                "iovation_data_details_device_blackboxMetadata_timestamp",
                "iovation_data_details_device_browser_configuredLanguage",
                "iovation_data_details_device_browser_cookiesEnabled",
                "iovation_data_details_device_browser_language",
                "iovation_data_details_device_browser_timezone",
                "iovation_data_details_device_browser_type",
                "iovation_data_details_device_browser_version",
                "iovation_data_details_device_firstSeen",
                "iovation_data_details_device_isNew",
                "iovation_data_details_device_os",
                "iovation_data_details_device_screen",
                "iovation_data_details_device_type",
                "iovation_data_details_realIp_address",
                "iovation_data_details_realIp_ipLocation_city",
                "iovation_data_details_realIp_ipLocation_country",
                "iovation_data_details_realIp_ipLocation_countryCode",
                "iovation_data_details_realIp_ipLocation_latitude",
                "iovation_data_details_realIp_ipLocation_longitude",
                "iovation_data_details_realIp_ipLocation_region",
                "iovation_data_details_realIp_isp",
                "iovation_data_details_realIp_parentOrganization",
                "iovation_data_details_realIp_source",
                "iovation_data_details_ruleResults_rulesMatched",
                "iovation_data_details_ruleResults_score",
                "iovation_data_details_statedIp_address",
                "iovation_data_details_statedIp_ipLocation_city",
                "iovation_data_details_statedIp_ipLocation_country",
                "iovation_data_details_statedIp_ipLocation_countryCode",
                "iovation_data_details_statedIp_ipLocation_latitude",
                "iovation_data_details_statedIp_ipLocation_longitude",
                "iovation_data_details_statedIp_ipLocation_region",
                "iovation_data_details_statedIp_isp",
                "iovation_data_details_statedIp_parentOrganization",
                "iovation_data_details_statedIp_source",
                "iovation_data_id",
                "iovation_data_reason",
                "iovation_data_result",
                "iovation_data_trackingNumber",
                "iovation_requestedAt",
                "metadata_context_traceId",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "custom_kyc_event_version",
            "event_id",
            "iovation_blackBox",
            "iovation_data_details_device_alias",
            "iovation_data_details_device_blackboxMetadata_age",
            "iovation_data_details_device_blackboxMetadata_timestamp",
            "iovation_data_details_device_browser_configuredLanguage",
            "iovation_data_details_device_browser_cookiesEnabled",
            "iovation_data_details_device_browser_language",
            "iovation_data_details_device_browser_timezone",
            "iovation_data_details_device_browser_type",
            "iovation_data_details_device_browser_version",
            "iovation_data_details_device_firstSeen",
            "iovation_data_details_device_isNew",
            "iovation_data_details_device_os",
            "iovation_data_details_device_screen",
            "iovation_data_details_device_type",
            "iovation_data_details_realIp_address",
            "iovation_data_details_realIp_ipLocation_city",
            "iovation_data_details_realIp_ipLocation_country",
            "iovation_data_details_realIp_ipLocation_countryCode",
            "iovation_data_details_realIp_ipLocation_latitude",
            "iovation_data_details_realIp_ipLocation_longitude",
            "iovation_data_details_realIp_ipLocation_region",
            "iovation_data_details_realIp_isp",
            "iovation_data_details_realIp_parentOrganization",
            "iovation_data_details_realIp_source",
            "iovation_data_details_ruleResults_rulesMatched",
            "iovation_data_details_ruleResults_score",
            "iovation_data_details_statedIp_address",
            "iovation_data_details_statedIp_ipLocation_city",
            "iovation_data_details_statedIp_ipLocation_country",
            "iovation_data_details_statedIp_ipLocation_countryCode",
            "iovation_data_details_statedIp_ipLocation_latitude",
            "iovation_data_details_statedIp_ipLocation_longitude",
            "iovation_data_details_statedIp_ipLocation_region",
            "iovation_data_details_statedIp_isp",
            "iovation_data_details_statedIp_parentOrganization",
            "iovation_data_details_statedIp_source",
            "iovation_data_id",
            "iovation_data_reason",
            "iovation_data_result",
            "iovation_data_trackingNumber",
            "iovation_requestedAt",
            "metadata_context_traceId",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}