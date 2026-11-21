
-- ============================================================
-- GOLD: VENTAS DIARIAS
-- Grano: fecha_negocio + pais + linea_negocio_corporativa
-- Fuente: silver_consolidada
-- ============================================================

CREATE OR REPLACE TEMP VIEW ventas_diarias AS

SELECT
    fecha_negocio,
    pais,
    linea_negocio_corporativa,

    SUM(
        CASE
            WHEN estado = 'APROBADA' THEN 1
            ELSE 0
        END
    ) AS trx_efectivas,

    SUM(
        CASE
            WHEN estado = 'APROBADA' THEN monto
            ELSE CAST(0 AS DECIMAL(18,2))
        END
    ) AS valor_efectivo_local,

    SUM(
        CASE
            WHEN estado = 'APROBADA' THEN comision
            ELSE CAST(0 AS DECIMAL(18,2))
        END
    ) AS ingreso_comision_local,

    SUM(
        CASE
            WHEN estado = 'REVERSADA' THEN 1
            ELSE 0
        END
    ) AS trx_reversadas,

    SUM(
        CASE
            WHEN estado = 'RECHAZADA' THEN 1
            ELSE 0
        END
    ) AS trx_rechazadas

FROM silver_consolidada

WHERE fecha_negocio IS NOT NULL
  AND pais IN ('CO', 'MX')
  AND linea_negocio_corporativa IS NOT NULL
  AND observacion_calidad IS NULL
  AND estado IN ('APROBADA', 'REVERSADA', 'RECHAZADA')

GROUP BY
    fecha_negocio,
    pais,
    linea_negocio_corporativa;
