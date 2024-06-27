{% macro return_config_co_f_kyc_bureau_income_estimator_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_bureau_income_estimator_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-12-09 11:00 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_bureau_income_estimator_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "prospectbureauincomeestimatorobtained": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "income_averageSMLV",
                "income_creditCardBalance",
                "income_creditCardInitialApprovedAmount",
                "income_creditCardInstallment",
                "income_estimatedIncome",
                "income_indebtednessCapacity",
                "income_maximum",
                "income_maximumSMLV",
                "income_minimum",
                "income_minimumSMLV",
                "income_nonRevolvingTotalBalance",
                "income_nonRevolvingTotalInitialApprovedAmount",
                "income_nonRevolvingTotalInstallment",
                "income_nonRevolvingTotalProducts",
                "income_paymentCapacity",
                "income_totalActiveCreditCards",
                "income_totalActiveProducts",
                "income_totalBalance",
                "income_totalInitialApprovedAmount",
                "income_totalInstallment",
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
            "income_averageSMLV",
            "income_creditCardBalance",
            "income_creditCardInitialApprovedAmount",
            "income_creditCardInstallment",
            "income_estimatedIncome",
            "income_indebtednessCapacity",
            "income_maximum",
            "income_maximumSMLV",
            "income_minimum",
            "income_minimumSMLV",
            "income_nonRevolvingTotalBalance",
            "income_nonRevolvingTotalInitialApprovedAmount",
            "income_nonRevolvingTotalInstallment",
            "income_nonRevolvingTotalProducts",
            "income_paymentCapacity",
            "income_totalActiveCreditCards",
            "income_totalActiveProducts",
            "income_totalBalance",
            "income_totalInitialApprovedAmount",
            "income_totalInstallment",
            "metadata_context_traceId",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}