{% macro return_config_co_d_client_geolocation_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_client_geolocation_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-10 06:15 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_client_geolocation_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "usergeolocationregistered": {
            "stage": "shop_discovery",
            "direct_attributes": [
                "event_id",
                "client_id",
                "device_id",
                "altitude",
                "altitude_accuracy",
                "latitude",
                "longitude",
                "geolocation_accuracy",
                "geolocation_heading",
                "geolocation_speed",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "altitude",
            "altitude_accuracy",
            "client_id",
            "device_id",
            "event_id",
            "geolocation_accuracy",
            "geolocation_heading",
            "geolocation_speed",
            "latitude",
            "longitude",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}