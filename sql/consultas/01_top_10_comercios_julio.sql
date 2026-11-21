
WITH crm_normalizado AS (

    SELECT
        pais,
        TRY_CAST(comercio_id AS BIGINT) AS comercio_id,
        nombre_comercial,
        segmento,
        ciudad

    FROM silver_crm_comercios
    WHERE es_version_actual = TRUE
),

ingresos_comercios AS (

    SELECT
        t.pais,

        TRY_CAST(
            REGEXP_REPLACE(
                TRIM(t.comercio_id),
                '^MX-',
                ''
            ) AS BIGINT
        ) AS comercio_id,

        COUNT(*) AS trx_efectivas,

        SUM(t.comision) AS ingreso_comision_local

    FROM silver_consolidada t

    WHERE t.fecha_negocio >= DATE '2026-07-01'
      AND t.fecha_negocio < DATE '2026-08-01'
      AND t.estado = 'APROBADA'
      AND t.observacion_calidad IS NULL

    GROUP BY
        t.pais,
        TRY_CAST(
            REGEXP_REPLACE(
                TRIM(t.comercio_id),
                '^MX-',
                ''
            ) AS BIGINT
        )
),

comercios_con_atributos AS (

    SELECT
        i.pais,
        i.comercio_id,
        c.nombre_comercial,
        c.segmento,
        c.ciudad,
        i.trx_efectivas,
        i.ingreso_comision_local

    FROM ingresos_comercios i

    LEFT JOIN crm_normalizado c
        ON i.pais = c.pais
       AND i.comercio_id = c.comercio_id
),

ranking AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY pais
            ORDER BY
                ingreso_comision_local DESC,
                comercio_id
        ) AS posicion

    FROM comercios_con_atributos
)

SELECT
    pais,
    posicion,
    comercio_id,
    nombre_comercial,
    segmento,
    ciudad,
    trx_efectivas,
    ingreso_comision_local

FROM ranking
WHERE posicion <= 10
ORDER BY pais, posicion;
