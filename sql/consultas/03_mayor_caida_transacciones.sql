
WITH transacciones_diarias AS (

    SELECT
        fecha_negocio,
        COUNT(*) AS total_transacciones

    FROM silver_consolidada

    WHERE fecha_negocio IS NOT NULL
      AND observacion_calidad IS NULL
      AND estado IN (
          'APROBADA',
          'RECHAZADA',
          'PENDIENTE',
          'REVERSADA'
      )

    GROUP BY fecha_negocio
),

comparacion AS (

    SELECT
        fecha_negocio,
        total_transacciones,
        DAYOFWEEK(fecha_negocio) AS dia_semana,

        (
            SUM(total_transacciones) OVER (
                PARTITION BY DAYOFWEEK(fecha_negocio)
            ) - total_transacciones
        )
        /
        NULLIF(
            COUNT(*) OVER (
                PARTITION BY DAYOFWEEK(fecha_negocio)
            ) - 1,
            0
        ) AS promedio_mismo_dia_semana

    FROM transacciones_diarias
),

caidas AS (

    SELECT
        fecha_negocio,
        dia_semana,
        total_transacciones,
        ROUND(promedio_mismo_dia_semana, 2)
            AS promedio_mismo_dia_semana,

        ROUND(
            promedio_mismo_dia_semana - total_transacciones,
            2
        ) AS caida_absoluta,

        ROUND(
            100.0 *
            (promedio_mismo_dia_semana - total_transacciones)
            / NULLIF(promedio_mismo_dia_semana, 0),
            2
        ) AS caida_porcentual

    FROM comparacion

    WHERE promedio_mismo_dia_semana > 0
      AND total_transacciones < promedio_mismo_dia_semana
)

SELECT *
FROM caidas
ORDER BY caida_absoluta DESC
LIMIT 1;
