
WITH silver_agrupado AS (
    SELECT
        fecha_negocio,
        pais,
        linea_negocio_corporativa,

        SUM(CASE
            WHEN estado = 'APROBADA' THEN 1
            ELSE 0
        END) AS trx_efectivas,

        SUM(CASE
            WHEN estado = 'APROBADA' THEN monto
            ELSE CAST(0 AS DECIMAL(18,2))
        END) AS valor_efectivo_local,

        SUM(CASE
            WHEN estado = 'APROBADA' THEN comision
            ELSE CAST(0 AS DECIMAL(18,2))
        END) AS ingreso_comision_local,

        SUM(CASE
            WHEN estado = 'REVERSADA' THEN 1
            ELSE 0
        END) AS trx_reversadas,

        SUM(CASE
            WHEN estado = 'RECHAZADA' THEN 1
            ELSE 0
        END) AS trx_rechazadas

    FROM silver_consolidada
    WHERE fecha_negocio IS NOT NULL
      AND pais IN ('CO', 'MX')
      AND linea_negocio_corporativa IS NOT NULL
      AND observacion_calidad IS NULL
      AND estado IN ('APROBADA', 'REVERSADA', 'RECHAZADA')

    GROUP BY
        fecha_negocio,
        pais,
        linea_negocio_corporativa
),

comparacion AS (
    SELECT
        COALESCE(s.fecha_negocio, g.fecha_negocio) AS fecha_negocio,
        COALESCE(s.pais, g.pais) AS pais,
        COALESCE(
            s.linea_negocio_corporativa,
            g.linea_negocio_corporativa
        ) AS linea_negocio_corporativa,

        COALESCE(s.trx_efectivas, 0) AS trx_efectivas_silver,
        COALESCE(g.trx_efectivas, 0) AS trx_efectivas_gold,

        COALESCE(s.valor_efectivo_local, 0) AS valor_silver,
        COALESCE(g.valor_efectivo_local, 0) AS valor_gold,

        COALESCE(s.ingreso_comision_local, 0) AS comision_silver,
        COALESCE(g.ingreso_comision_local, 0) AS comision_gold,

        COALESCE(s.trx_reversadas, 0) AS reversadas_silver,
        COALESCE(g.trx_reversadas, 0) AS reversadas_gold,

        COALESCE(s.trx_rechazadas, 0) AS rechazadas_silver,
        COALESCE(g.trx_rechazadas, 0) AS rechazadas_gold

    FROM silver_agrupado s
    FULL OUTER JOIN ventas_diarias g
        ON s.fecha_negocio = g.fecha_negocio
       AND s.pais = g.pais
       AND s.linea_negocio_corporativa =
           g.linea_negocio_corporativa
)

SELECT
    *,
    trx_efectivas_silver - trx_efectivas_gold AS diferencia_trx,
    valor_silver - valor_gold AS diferencia_valor,
    comision_silver - comision_gold AS diferencia_comision,
    reversadas_silver - reversadas_gold AS diferencia_reversadas,
    rechazadas_silver - rechazadas_gold AS diferencia_rechazadas

FROM comparacion

WHERE trx_efectivas_silver <> trx_efectivas_gold
   OR ABS(valor_silver - valor_gold) > 0.01
   OR ABS(comision_silver - comision_gold) > 0.01
   OR reversadas_silver <> reversadas_gold
   OR rechazadas_silver <> rechazadas_gold

ORDER BY fecha_negocio, pais, linea_negocio_corporativa;
