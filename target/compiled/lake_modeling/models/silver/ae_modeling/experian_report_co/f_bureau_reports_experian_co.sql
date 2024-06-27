WITH full_source AS (SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_201911_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_201912_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202001_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202002_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202003_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202004_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202005_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202006_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202007_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202008_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202009_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202010_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202011_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202012_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202101_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202102_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202103_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202104_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202105_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202106_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202107_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202108_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202109_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202110_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202111_v2_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202112_v2_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202201_v2_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202202_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202203_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202204_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202205_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202206_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202207_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202208_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202209_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202210_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202211_co

UNION ALL


SELECT
    id_number,
    num_obl,
    name,
    dpd,
    novedad,
    saldo_mora,
    fecha_estado,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN '2019-12-10'
    
    WHEN fecha_estado = '20191231'
    THEN '2020-01-10'
    
    WHEN fecha_estado = '20200131'
    THEN '2020-02-10'
    
    WHEN fecha_estado = '20200229'
    THEN '2020-03-10'
    
    WHEN fecha_estado = '20200331'
    THEN '2020-04-10'
    
    WHEN fecha_estado = '20200430'
    THEN '2020-05-10'
    
    WHEN fecha_estado = '20200531'
    THEN '2020-06-10'
    
    WHEN fecha_estado = '20200630'
    THEN '2020-07-10'
    
    WHEN fecha_estado = '20200731'
    THEN '2020-08-10'
    
    WHEN fecha_estado = '20200831'
    THEN '2020-09-10'
    
    WHEN fecha_estado = '20200930'
    THEN '2020-10-10'
    
    WHEN fecha_estado = '20201031'
    THEN '2020-11-10'
    
    WHEN fecha_estado = '20201130'
    THEN '2020-12-10'
    
    WHEN fecha_estado = '20201231'
    THEN '2021-01-10'
    
    WHEN fecha_estado = '20210131'
    THEN '2021-02-10'
    
    WHEN fecha_estado = '20210228'
    THEN '2021-03-16'
    
    WHEN fecha_estado = '20210331'
    THEN '2021-04-12'
    
    WHEN fecha_estado = '20210430'
    THEN '2021-05-11'
    
    WHEN fecha_estado = '20210531'
    THEN '2021-06-18'
    
    WHEN fecha_estado = '20210630'
    THEN '2021-07-13'
    
    WHEN fecha_estado = '20210731'
    THEN '2021-08-20'
    
    WHEN fecha_estado = '20210831'
    THEN '2021-09-15'
    
    WHEN fecha_estado = '20210930'
    THEN '2021-10-12'
    
    WHEN fecha_estado = '20211031'
    THEN '2021-11-12'
    
    WHEN fecha_estado = '20211130'
    THEN '2021-12-14'
    
    WHEN fecha_estado = '20211231'
    THEN '2022-01-12'
    
    WHEN fecha_estado = '20220131'
    THEN '2022-02-24'
    
    WHEN fecha_estado = '20220228'
    THEN '2022-03-21'
    
    WHEN fecha_estado = '20220331'
    THEN '2022-04-19'
    
    WHEN fecha_estado = '20220430'
    THEN '2022-05-14'
    
    WHEN fecha_estado = '20220531'
    THEN '2022-06-13'
    
    WHEN fecha_estado = '20220630'
    THEN '2022-07-13'
    
    WHEN fecha_estado = '20220731'
    THEN '2022-08-12'
    
    WHEN fecha_estado = '20220831'
    THEN '2022-09-16'
    
END AS report_date,
    CASE
    
    WHEN fecha_estado = '20191130'
    THEN 'Noviembre-19'
    
    WHEN fecha_estado = '20191231'
    THEN 'Diciembre-19'
    
    WHEN fecha_estado = '20200131'
    THEN 'Enero-20'
    
    WHEN fecha_estado = '20200229'
    THEN 'Febrero-20'
    
    WHEN fecha_estado = '20200331'
    THEN 'Marzo-20'
    
    WHEN fecha_estado = '20200430'
    THEN 'Abril-20'
    
    WHEN fecha_estado = '20200531'
    THEN 'Mayo-20'
    
    WHEN fecha_estado = '20200630'
    THEN 'Junio-20'
    
    WHEN fecha_estado = '20200731'
    THEN 'Julio-20'
    
    WHEN fecha_estado = '20200831'
    THEN 'Agosto-20'
    
    WHEN fecha_estado = '20200930'
    THEN 'Septiembre-20'
    
    WHEN fecha_estado = '20201031'
    THEN 'Octubre-20'
    
    WHEN fecha_estado = '20201130'
    THEN 'Noviembre-20'
    
    WHEN fecha_estado = '20201231'
    THEN 'Diciembre-20'
    
    WHEN fecha_estado = '20210131'
    THEN 'Enero-21'
    
    WHEN fecha_estado = '20210228'
    THEN 'Febrero-21'
    
    WHEN fecha_estado = '20210331'
    THEN 'Marzo-21'
    
    WHEN fecha_estado = '20210430'
    THEN 'Abril-21'
    
    WHEN fecha_estado = '20210531'
    THEN 'Mayo-21'
    
    WHEN fecha_estado = '20210630'
    THEN 'Junio-21'
    
    WHEN fecha_estado = '20210731'
    THEN 'Julio-21'
    
    WHEN fecha_estado = '20210831'
    THEN 'Agosto-21'
    
    WHEN fecha_estado = '20210930'
    THEN 'Septiembre-21'
    
    WHEN fecha_estado = '20211031'
    THEN 'Octubre-21'
    
    WHEN fecha_estado = '20211130'
    THEN 'Noviembre-21'
    
    WHEN fecha_estado = '20211231'
    THEN 'Diciembre-21'
    
    WHEN fecha_estado = '20220131'
    THEN 'Enero-22'
    
    WHEN fecha_estado = '20220228'
    THEN 'Febrero-22'
    
    WHEN fecha_estado = '20220331'
    THEN 'Marzo-22'
    
    WHEN fecha_estado = '20220430'
    THEN 'Abril-22'
    
    WHEN fecha_estado = '20220531'
    THEN 'Mayo-22'
    
    WHEN fecha_estado = '20220630'
    THEN 'Junio-22'
    
    WHEN fecha_estado = '20220731'
    THEN 'Julio-22'
    
    WHEN fecha_estado = '20220831'
    THEN 'Agosto-22'
    
END AS reported_month
FROM bronze.experian_report_rep_202212_co


)

SELECT * FROM full_source;