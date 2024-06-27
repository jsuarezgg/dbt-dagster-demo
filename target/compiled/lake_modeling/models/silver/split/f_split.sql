

WITH bronze_split AS (

    SELECT
        md5(cast(concat(coalesce(cast(key as 
    string
), ''), '-', coalesce(cast(splitName as 
    string
), ''), '-', coalesce(cast(treatment as 
    string
), ''), '-', coalesce(cast(to_date(timestamp) as 
    string
), '')) as 
    string
)) AS id,
        md5(cast(concat(coalesce(cast(key as 
    string
), ''), '-', coalesce(cast(splitName as 
    string
), '')) as 
    string
)) AS client_split_key,
        splitName as split_name,
        key,
        timestamp,
        to_date(timestamp) as date,
        label,
        treatment
    FROM bronze.split_io

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


-- DBT INCREMENTAL SENTENCE

AND date =  to_date("2022-01-01")

