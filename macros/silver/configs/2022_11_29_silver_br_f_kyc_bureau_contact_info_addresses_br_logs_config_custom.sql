{% macro return_config_br_f_kyc_bureau_contact_info_addresses_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_bureau_contact_info_addresses_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-29 20:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_bureau_contact_info_addresses_br_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "prospectbureaucontactinfoobtained_unnested_by_address": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "bureau_address_addressComplement",
                "bureau_address_city",
                "bureau_address_country",
                "bureau_address_district",
                "bureau_address_firstReport",
                "bureau_address_homeNumber",
                "bureau_address_homeStreet",
                "bureau_address_order",
                "bureau_address_stateAcronym",
                "bureau_address_street",
                "bureau_address_type",
                "bureau_address_zipCode",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "array_parent_path",
            "bureau_address_addressComplement",
            "bureau_address_city",
            "bureau_address_country",
            "bureau_address_district",
            "bureau_address_firstReport",
            "bureau_address_homeNumber",
            "bureau_address_homeStreet",
            "bureau_address_order",
            "bureau_address_stateAcronym",
            "bureau_address_street",
            "bureau_address_type",
            "bureau_address_zipCode",
            "client_id",
            "event_id",
            "item_pseudo_idx",
            "ocurred_on",
            "surrogate_key"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}