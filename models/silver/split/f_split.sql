{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key="id",
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH bronze_split AS (

    SELECT
        {{ dbt_utils.surrogate_key(['key', 'splitName', 'treatment', 'to_date(timestamp)']) }} AS id,
        {{ dbt_utils.surrogate_key(['key', 'splitName']) }} AS client_split_key,
        splitName as split_name,
        key,
        timestamp,
        to_date(timestamp) as date,
        label,
        treatment
    FROM {{ source('bronze', 'split_io') }}

),
bronze_split_numbered AS (

    SELECT
        id,
        client_split_key,
        split_name,
        key,
        timestamp,
        date,
        label,
        treatment,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS row_number
    FROM bronze_split

)

SELECT
    id,
    client_split_key,
    split_name,
    key,
    timestamp,
    date,
    label,
    treatment
FROM bronze_split_numbered
WHERE row_number=1

{% if is_incremental() %}
-- DBT INCREMENTAL SENTENCE

AND date =  to_date("{{ var('start_date') }}")

{% endif %}
