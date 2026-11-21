
WITH transacciones_julio AS (

    SELECT DISTINCT
        pais,

        TRY_CAST(
            REGEXP_REPLACE(
                TRIM(comercio_id),
                '^MX-',
                ''
            ) AS BIGINT
        ) AS comercio_id

    FROM silver_consolidada

    WHERE fecha_negocio >= DATE '2026-07-01'
      AND fecha_negocio < DATE '2026-08-01'
      AND observacion_calidad IS NULL
),

crm_normalizado AS (

    SELECT
        pais,
        TRY_CAST(comercio_id AS BIGINT) AS comercio_id,
        nombre_comercial,
        documento_tributario,
        segmento,
        ciudad,
        region,
        estado,
        plan_comision

    FROM silver_crm_comercios

    WHERE es_version_actual = TRUE
      AND estado = 'ACTIVO'
)

SELECT
    c.pais,
    c.comercio_id,
    c.nombre_comercial,
    c.segmento,
    c.ciudad,
    c.region,
    c.plan_comision

FROM crm_normalizado c

LEFT ANTI JOIN transacciones_julio t
    ON c.pais = t.pais
   AND c.comercio_id = t.comercio_id

ORDER BY
    c.pais,
    c.comercio_id;
