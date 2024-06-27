{% snapshot bronze_unified_column_catalog %}
    {{
        config(
          target_schema='bronze',
          file_format='delta',
          strategy='timestamp',
      	  unique_key='surrogate_id',
          updated_at='db_updated_at')
    }}

SELECT {{ dbt_utils.surrogate_key(['table_schema', 'table_name','column_name']) }} AS surrogate_id, *
FROM bronze.unified_column_catalog
{% endsnapshot %}