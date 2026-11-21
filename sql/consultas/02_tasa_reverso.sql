
SELECT
    pais,
    linea_negocio_corporativa,

    SUM(
        CASE WHEN estado = 'REVERSADA'
             THEN 1 ELSE 0 END
    ) AS trx_reversadas,

    SUM(
        CASE WHEN estado = 'APROBADA'
             THEN 1 ELSE 0 END
    ) AS trx_efectivas,

    SUM(
        CASE
            WHEN estado IN ('APROBADA', 'REVERSADA')
            THEN 1 ELSE 0
        END
    ) AS trx_base_reverso,

    ROUND(
        100.0 *
        SUM(CASE WHEN estado = 'REVERSADA' THEN 1 ELSE 0 END)
        /
        NULLIF(
            SUM(
                CASE
                    WHEN estado IN ('APROBADA', 'REVERSADA')
                    THEN 1 ELSE 0
                END
            ),
            0
        ),
        2
    ) AS tasa_reverso_pct

FROM silver_consolidada

WHERE fecha_negocio IS NOT NULL
  AND observacion_calidad IS NULL
  AND linea_negocio_corporativa IS NOT NULL
  AND estado IN ('APROBADA', 'REVERSADA')

GROUP BY
    pais,
    linea_negocio_corporativa

ORDER BY
    pais,
    tasa_reverso_pct DESC;
