{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--- QUERY CREATED BY:  RISK TEAM - MANOEL BOMFIM (BR) + EXTERNAL (PROCLINK) - Recommendations and overview by Carlos D.A. Puerto N. (AE - Data team)
--- Description: This datamart is part of the process of deploying Looker across the entire company (2023Q1)
--- LAST UPDATE: @2023-02-23

WITH score_data AS (
(
    select
        to_date(current_execution_date) AS current_execution_date,
        client_id,
        proba_pd_rc
    from hive_metastore.ds.back_test_prediction_202202_202211_rc_co_v2
    )
    union all
    (
    select
        to_date(current_execution_date) AS current_execution_date,
        client_id,
        proba_pd_rc
    from {{ source('risk','predictions_returning_client_model_cl_level') }}
    where to_date(current_execution_date) > '2022-11-09'
    )
)

select
    'co' AS country_code,
    a.client_id,
    a.calculation_date as daily_client_data_calculation_date,
    MAX(b.current_execution_date) AS date_most_recent_score_available,
    element_at(array_sort(array_agg(CASE WHEN b.proba_pd_rc is not null then struct(b.current_execution_date, b.proba_pd_rc) else NULL end), (left, right) -> case when left.current_execution_date < right.current_execution_date then 1 when left.current_execution_date > right.current_execution_date then -1 when left.current_execution_date == right.current_execution_date then 0 end), 1).proba_pd_rc as most_recen_proba_pd_rc,
    FIRST_VALUE(NOW()) AS ingested_at,
    FIRST_VALUE(to_timestamp('{{ var("execution_date") }}')) AS updated_at
from       {{ source('risk','daily_client_data_co') }} AS a
INNER JOIN                score_data b on a.client_id = b.client_id and a.calculation_date >= b.current_execution_date and datediff(a.calculation_date,b.current_execution_date) < 40
GROUP BY 1,2,3
