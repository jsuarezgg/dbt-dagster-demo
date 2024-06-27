{% macro return_config_co_d_ally_cms_integration_monitoring_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_ally_cms_integration_monitoring_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-10-24 11:08 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_ally_cms_integration_monitoring_co",
        "files_db_table_pks": [
            "ally_slug"
        ]
    },
    "events": {
        "allycmsintegrationmonitoringupdated": {
            "direct_attributes": [
                "ally_slug",
                "monitoring_checkout_integration_status",
                "monitoring_checkout_integration_updated_at",
                "monitoring_checkout_traffic_status",
                "monitoring_checkout_traffic_updated_at",
                "monitoring_status",
                "monitoring_updated_at",
                "monitoring_widget_integration_status",
                "monitoring_widget_integration_updated_at",
                "monitoring_widget_traffic_status",
                "monitoring_widget_traffic_updated_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "monitoring_checkout_integration_status",
            "monitoring_checkout_integration_updated_at",
            "monitoring_checkout_traffic_status",
            "monitoring_checkout_traffic_updated_at",
            "monitoring_status",
            "monitoring_updated_at",
            "monitoring_widget_integration_status",
            "monitoring_widget_integration_updated_at",
            "monitoring_widget_traffic_status",
            "monitoring_widget_traffic_updated_at",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}