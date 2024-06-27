{{
    config(
        materialized='table',
        tags=["data_usage_and_costs"],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{%- set tables_list = run_query("""

SELECT
    amplitude_table_name AS table
FROM bronze.amplitude_tables_catalog
WHERE 1=1
--AND amplitude_table_name LIKE 'amplitude_addi_funnel_%'
AND amplitude_table_name NOT IN ('amplitude_addi_funnel_raw_backup', 'amplitude_tables_catalog')

""") -%}

{% for table in tables_list %}
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('{{ var("execution_date") }}')) AS updated_at
    FROM bronze.{{table['table']}}
    GROUP BY 2
    
    {% if not loop.last -%}
    UNION ALL
    {% endif -%}
{% endfor %}
