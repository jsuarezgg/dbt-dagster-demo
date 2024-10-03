{% macro return_config_co_f_applications_discounts_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_discounts_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-07-09 11:57 TZ-0600",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_discounts_co_logs",
        "files_db_table_pks": [
            "custom_event_discount_idx_pairing_id"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_discounts": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "store_slug",
                "channel",
                "product",
                "custom_platform_version",
                "order_id",
                "total_discount_percentage_assumed_by_addi",
                "total_discount_percentage_assumed_by_ally",
                "total_discount_percentage",
                "discount_percentage_assumed_by_addi",
                "discount_percentage_assumed_by_ally",
                "discount_amount",
                "discount_percentage",
                "discount_sub_order_id",
                "discount_type",
                "custom_event_discount_idx_pairing_id",
                "custom_application_discount_idx_pairing_id"
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
            "custom_application_discount_idx_pairing_id",
            "custom_event_discount_idx_pairing_id",
            "custom_platform_version",
            "discount_amount",
            "discount_percentage",
            "discount_percentage_assumed_by_addi",
            "discount_percentage_assumed_by_ally",
            "discount_sub_order_id",
            "discount_type",
            "ocurred_on",
            "order_id",
            "product",
            "store_slug",
            "total_discount_percentage",
            "total_discount_percentage_assumed_by_addi",
            "total_discount_percentage_assumed_by_ally"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
