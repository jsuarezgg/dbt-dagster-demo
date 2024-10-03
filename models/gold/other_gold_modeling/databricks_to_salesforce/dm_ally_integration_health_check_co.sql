{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH f_ally_store_health_checks_co AS (
  SELECT
    ally_slug,
    healthcheck_checkout_methods,
    healthcheck_checkout_position,
    healthcheck_component,
    healthcheck_datetime
  FROM {{ source('silver_live', 'f_ally_store_health_checks_co') }}
)
,
d_ally_cms_integration_monitoring_co AS (
	SELECT
	  ally_slug,
	  monitoring_checkout_integration_status,
	  monitoring_checkout_integration_updated_at,
	  monitoring_checkout_traffic_status,
	  monitoring_checkout_traffic_updated_at,
	  monitoring_status,
	  monitoring_updated_at,
	  monitoring_widget_integration_status,
	  monitoring_widget_integration_updated_at,
	  monitoring_widget_traffic_status,
	  monitoring_widget_traffic_updated_at
	FROM {{ source('silver_live', 'd_ally_cms_integration_monitoring_co') }}
)
SELECT
	d.ally_slug,
	d.monitoring_checkout_integration_status,
	d.monitoring_checkout_integration_updated_at,
	d.monitoring_checkout_traffic_status,
	d.monitoring_checkout_traffic_updated_at,
	d.monitoring_status,
	d.monitoring_updated_at,
	d.monitoring_widget_integration_status,
	d.monitoring_widget_integration_updated_at,
	d.monitoring_widget_traffic_status,
	d.monitoring_widget_traffic_updated_at,
	f.healthcheck_checkout_methods,
	f.healthcheck_checkout_position,
	f.healthcheck_component,
	f.healthcheck_datetime
FROM d_ally_cms_integration_monitoring_co d
LEFT JOIN f_ally_store_health_checks_co f 	ON d.ally_slug = f.ally_slug