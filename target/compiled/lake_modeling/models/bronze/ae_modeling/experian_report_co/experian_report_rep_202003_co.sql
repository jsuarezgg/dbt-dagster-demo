


SELECT
    -- MANDATORY FIELDS
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.experian_report_rep_202003_co