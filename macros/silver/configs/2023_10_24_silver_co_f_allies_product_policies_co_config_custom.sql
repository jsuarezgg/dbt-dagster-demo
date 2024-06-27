{% macro return_config_co_f_allies_product_policies_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_allies_product_policies_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-10-26 13:38 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_allies_product_policies_co",
        "files_db_table_pks": [
            "application_product_policy_id"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_productpolicies": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_product_policy_id",
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "store_slug",
                "order_id",
                "channel",
                "product",
                "custom_platform_version",
                "cancellation_mdf",
                "fraud_mdf",
                "origination_mdf",
                "type",
                "max_amount"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "application_product_policy_id",
            "cancellation_mdf",
            "channel",
            "client_id",
            "custom_platform_version",
            "fraud_mdf",
            "max_amount",
            "ocurred_on",
            "order_id",
            "origination_mdf",
            "product",
            "store_slug",
            "type"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}