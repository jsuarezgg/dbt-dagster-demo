{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH raw_rejection_rules AS (
	SELECT
		application_id,
		iovation_rule_result_score,
		NAMED_STRUCT('type', iovation_rule_result_type, 'score', iovation_rule_result_score, 'reason', iovation_rule_result_reason) AS iovation_rule_results_rules,
		CASE WHEN iovation_rule_result_score = -100 THEN 1 END AS flag_rejection_rule,
        DENSE_RANK () OVER(PARTITION BY application_id ORDER BY ocurred_on DESC) AS priority
	FROM {{ ref('f_kyc_iovation_v1v2_result_rules_br_logs') }}
)
,
rule_result_rules_by_app AS (
	SELECT
		application_id,
		COLLECT_LIST(iovation_rule_results_rules) AS rule_results_rules,
		COLLECT_LIST(iovation_rule_results_rules) FILTER (WHERE flag_rejection_rule = 1) AS rejection_rule_results_rules,
		COLLECT_LIST(iovation_rule_results_rules.score) FILTER (WHERE flag_rejection_rule = 1) AS rejection_rule_results_rules_int,
		SUM(iovation_rule_result_score) FILTER (WHERE flag_rejection_rule = 1) AS rejection_rule_results_score,
		COUNT(iovation_rule_result_score) FILTER (WHERE flag_rejection_rule = 1) AS rejection_rule_results_rules_matched
	FROM raw_rejection_rules
    WHERE priority = 1
	GROUP BY 1
)
SELECT
	io.application_id,
	io.iovation_data_result AS io_result,
	io.iovation_data_reason AS io_reason,
	io.iovation_data_trackingNumber AS io_tracking_number,
	io.iovation_data_details_device_os AS io_device_os,
	io.iovation_data_details_device_type AS io_device_type,
	io.iovation_data_details_device_alias AS io_device_alias,
	io.iovation_data_details_device_screen AS io_device_screen,
	io.iovation_data_details_device_browser_type AS io_device_browser_type,
	io.iovation_data_details_device_browser_version AS io_device_browser_version,
	io.iovation_data_details_device_firstSeen AS io_device_firstSeen,
	io.iovation_data_details_statedIp_isp AS io_stated_ip_isp,
	io.iovation_data_details_statedIp_address AS io_stated_ip_address,
	io.iovation_data_details_statedIp_ipLocation_city AS io_stated_ip_ipLocation_city,
	io.iovation_data_details_statedIp_ipLocation_region AS io_stated_ip_ipLocation_region,
	io.iovation_data_details_statedIp_ipLocation_country AS io_stated_ip_ipLocation_country,
	io.iovation_data_details_realIp_isp AS io_real_ip_isp,
	io.iovation_data_details_realIp_address AS io_real_ip_address,
	io.iovation_data_details_realIp_ipLocation_city AS io_real_ip_ipLocation_city,
	io.iovation_data_details_realIp_ipLocation_region AS io_real_ip_ipLocation_region,
	io.iovation_data_details_realIp_ipLocation_country AS io_real_ip_ipLocation_country,
	io.iovation_data_details_ruleResults_score AS io_rule_results_score,
	io.iovation_data_details_ruleResults_rulesMatched AS io_rule_results_rulesMatched,
	rrr.rule_results_rules AS io_rule_results_rules,
	rrr.rejection_rule_results_rules AS io_rejection_rule_results_rules,
	rrr.rejection_rule_results_rules_int AS io_rejection_rule_results_rules_int,
	rrr.rejection_rule_results_score AS io_rejection_rule_results_score,
	rrr.rejection_rule_results_rules_matched AS io_rejection_rule_results_rulesMatched
FROM {{ ref('f_kyc_iovation_v1v2_br') }} io
LEFT JOIN rule_result_rules_by_app rrr		ON io.application_id = rrr.application_id
