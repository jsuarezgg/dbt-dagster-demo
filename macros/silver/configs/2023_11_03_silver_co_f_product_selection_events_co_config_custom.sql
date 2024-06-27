{% macro return_config_co_f_product_selection_events_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_product_selection_events_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-05-10 16:08 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_product_selection_events_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "productselected": {
            "reference_order_id": 1,
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "ally_slug",
                "channel",
                "order_id"
            ],
            "custom_attributes": {}
        },
        "productselectionrequired": {
            "reference_order_id": 2,
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "ally_slug",
                "client_type",
                "channel",
                "order_id",
                "auto_selection"
            ],
            "custom_attributes": {}
        },
        "productselectiondeclined": {
            "reference_order_id": 3,
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "ally_slug",
                "client_type",
                "channel",
                "order_id"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "auto_selection",
            "channel",
            "client_id",
            "client_type",
            "ocurred_on",
            "order_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}