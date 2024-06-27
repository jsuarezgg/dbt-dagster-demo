{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- raw.ally_management_ally_payments_co

WITH explode_taxes AS (
    SELECT
    id,
    explode(from_json(data:tax:taxes,'array<string>')) AS tax
    FROM {{ ref('ally_management_ally_payments_co') }}
),

subexplode_taxes AS (
SELECT
    id,
    explode(from_json(tax:values,'array<string>')) AS tax_type_detail
    FROM explode_taxes
),

agg_table(
SELECT
    id,
    sum(COALESCE(tax_type_detail:amount::double,0)) FILTER (WHERE tax_type_detail:type='IVA_CO') as iva_co,
    sum(COALESCE(tax_type_detail:amount::double,0)) FILTER (WHERE tax_type_detail:type='RETEIVA_CO') as reteiva_co,
    sum(COALESCE(tax_type_detail:amount::double,0)) FILTER (WHERE tax_type_detail:type='RETEFUENTE_CO') as retefuente_co,
    sum(COALESCE(tax_type_detail:amount::double,0)) FILTER (WHERE tax_type_detail:type='RETEICA_CO') as reteica_co
FROM subexplode_taxes
GROUP BY id)

SELECT     
    st.id,
    st.ally_group_id,
    st.ally_slug,
    st.type,
    st.total,
    st.anticipation_fee,
    st.anticipated,
    st.status,
    st.occurred_on,
    st.start_date,
    st.end_date,
    st.scheduled_payment_date,
    st.payment_date,
    st.data:reportTerm AS report_term,
    st.data:paymentTerm AS payment_term,
    st.data,
    at.iva_co,
    at.reteiva_co,
    at.retefuente_co,
    at.reteica_co,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_payments_co') }} st
LEFT JOIN agg_table at ON at.id=st.id
