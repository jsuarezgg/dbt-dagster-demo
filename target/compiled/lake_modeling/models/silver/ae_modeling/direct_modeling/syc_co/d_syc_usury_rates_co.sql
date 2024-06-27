


--bronze.syc_usury_rates_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    official_usury_rate,
    usury_rate,
    on_overdue_principal_rate,
    official_low_balance_usury_rate,
    low_balance_usury_rate,
    start_date,
    end_date,
    created_at,
    created_by,
    updated_at_source,
    updated_by,
    ingested_at,
    updated_at

-- DBT SOURCE REFERENCE
FROM bronze.syc_usury_rates_co