{% macro return_config_br_f_applications_order_details_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_applications_order_details_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-07 15:06 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_applications_order_details_br_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "ApplicationCreated_unnested_by_items": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "surrogate_key",
                "event_name",
                "event_id",
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "channel",
                "order_id",
                "product",
                "item_brand",
                "item_category",
                "item_discount",
                "item_name",
                "item_pictureurl",
                "item_quantity",
                "item_sku",
                "item_taxamount",
                "item_unitprice"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "channel",
            "client_id",
            "event_id",
            "event_name",
            "item_brand",
            "item_category",
            "item_discount",
            "item_name",
            "item_pictureurl",
            "item_quantity",
            "item_sku",
            "item_taxamount",
            "item_unitprice",
            "ocurred_on",
            "order_id",
            "product",
            "surrogate_key"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}