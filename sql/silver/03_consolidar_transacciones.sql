
CREATE OR REPLACE TEMP VIEW silver_consolidada AS

WITH transacciones_unificadas AS (

    -- 1. Unificar Colombia y México
    SELECT * FROM transacciones_colombia

    UNION ALL

    SELECT * FROM transacciones_mexico
),

productos_catalogo AS (

    -- 2. Normalizar los campos del catálogo de productos
    SELECT
        UPPER(TRIM(pais)) AS pais,
        TRIM(producto_cod) AS producto_cod,
        TRIM(sku_origen) AS sku_origen,
        TRIM(nombre) AS nombre_producto,
        TRIM(linea_negocio) AS linea_negocio_producto,
        CAST(limites.monto_min AS DECIMAL(18,2)) AS monto_minimo,
        CAST(limites.monto_max AS DECIMAL(18,2)) AS monto_maximo,
        TO_DATE(vigente_desde) AS vigente_desde,
        TO_DATE(vigente_hasta) AS vigente_hasta
    FROM catalogo_productos
),

productos_cruzados AS (

    -- 3. Relacionar cada transacción con el producto vigente
    SELECT
        t.*,

        p.producto_cod AS producto_codigo_catalogo,
        p.nombre_producto,
        p.linea_negocio_producto,
        p.monto_minimo,
        p.monto_maximo,
        p.vigente_desde,
        p.vigente_hasta,

        ROW_NUMBER() OVER (
            PARTITION BY t.pais, t.id_transaccion
            ORDER BY
                p.vigente_desde DESC NULLS LAST,
                p.producto_cod
        ) AS rn_producto

    FROM transacciones_unificadas t

    LEFT JOIN productos_catalogo p
        ON t.pais = p.pais

        AND (
            (
                t.pais = 'CO'
                AND TRIM(t.producto_codigo_origen) = p.producto_cod
            )
            OR
            (
                t.pais = 'MX'
                AND TRIM(t.producto_codigo_origen) = p.sku_origen
            )
        )

        AND t.fecha_negocio >= p.vigente_desde
        AND (
            p.vigente_hasta IS NULL
            OR t.fecha_negocio <= p.vigente_hasta
        )
),

producto_unico AS (

    SELECT *
    FROM productos_cruzados
    WHERE rn_producto = 1
),

canales_normalizados AS (

    -- 4. Normalizar las llaves del catálogo de canales
    SELECT
        UPPER(TRIM(pais)) AS pais,
        UPPER(TRIM(codigo_origen)) AS codigo_origen,
        UPPER(TRIM(canal_homologado)) AS canal_homologado
    FROM catalogo_canales
),

canales_cruzados AS (

    -- 5. Homologar los canales
    SELECT
        p.*,
        c.canal_homologado

    FROM producto_unico p

    LEFT JOIN canales_normalizados c
        ON p.pais = c.pais
        AND UPPER(TRIM(p.canal_codigo_origen)) = c.codigo_origen
),

lineas_mapeadas AS (

    -- 6. Homologar las líneas propias de cada país
    SELECT
        *,

        CASE
            WHEN pais = 'CO'
                THEN linea_negocio_producto

            WHEN pais = 'MX'
                AND UPPER(TRIM(linea_negocio_producto)) = 'RECARGAS TIEMPO AIRE'
                THEN 'Recargas'

            WHEN pais = 'MX'
                AND UPPER(TRIM(linea_negocio_producto)) = 'PAGO DE SERVICIOS'
                THEN 'Recaudos'

            WHEN pais = 'MX'
                AND UPPER(TRIM(linea_negocio_producto)) = 'TARJETAS DE REGALO'
                THEN 'Pines y entretenimiento'

            WHEN pais = 'MX'
                AND UPPER(TRIM(linea_negocio_producto)) = 'CORRESPONSALIA'
                THEN 'Corresponsalia bancaria'

            WHEN pais = 'MX'
                AND UPPER(TRIM(linea_negocio_producto)) = 'MOVILIDAD'
                THEN 'Movilidad'

            ELSE NULL
        END AS linea_negocio_corporativa_propuesta

    FROM canales_cruzados
),

taxonomia_normalizada AS (

    -- 7. Taxonomía corporativa oficial
    SELECT DISTINCT
        TRIM(linea_negocio_corporativa) AS linea_taxonomia
    FROM catalogo_taxonomia
),

lineas_validadas AS (

    -- 8. Comprobar que la línea propuesta exista en la taxonomía
    SELECT
        l.*,

        CASE
            WHEN tx.linea_taxonomia IS NOT NULL
                THEN tx.linea_taxonomia
            ELSE NULL
        END AS linea_negocio_corporativa

    FROM lineas_mapeadas l

    LEFT JOIN taxonomia_normalizada tx
        ON TRIM(l.linea_negocio_corporativa_propuesta)
            = tx.linea_taxonomia
),

indicadores_calidad AS (

    -- 9. Calcular indicadores de calidad
    SELECT
        *,
        
        CASE
            WHEN estado IN (
                'APROBADA',
                'RECHAZADA',
                'PENDIENTE',
                'REVERSADA'
            )
            THEN TRUE
            ELSE FALSE
        END AS es_estado_valido,

        CASE
            WHEN fecha_hora_negocio IS NOT NULL
                AND fecha_negocio IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS es_fecha_valida,

        CASE
            WHEN monto IS NOT NULL AND monto >= 0
            THEN TRUE
            ELSE FALSE
        END AS es_monto_valido,

        CASE
            WHEN comision IS NOT NULL AND comision >= 0
            THEN TRUE
            ELSE FALSE
        END AS es_comision_valida,

        CASE
            WHEN canal_homologado IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS es_canal_homologado,
        CASE
            WHEN pais = 'CO'
                AND UPPER(TRIM(producto_codigo_origen)) = 'TST-001'
            THEN TRUE
            ELSE FALSE
        END AS es_producto_prueba,
        CASE
            WHEN producto_codigo_catalogo IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS es_producto_homologado,

        CASE
            WHEN linea_negocio_corporativa IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS es_linea_homologada,

        CASE
            WHEN producto_codigo_catalogo IS NOT NULL
                AND monto IS NOT NULL
                AND monto >= monto_minimo
                AND monto <= monto_maximo
            THEN TRUE
            ELSE FALSE
        END AS es_monto_en_rango_producto

    FROM lineas_validadas
),

calidad_final AS (

    -- 10. Asignar una observación prioritaria por registro
    SELECT
        *,

        CASE
            WHEN es_estado_valido = FALSE
                THEN 'ESTADO_INVALIDO'

            WHEN es_fecha_valida = FALSE
                THEN 'FECHA_INVALIDA'

            WHEN es_monto_valido = FALSE
                THEN 'MONTO_INVALIDO'

            WHEN es_comision_valida = FALSE
                THEN 'COMISION_INVALIDA'

            WHEN es_canal_homologado = FALSE
                THEN 'CANAL_NO_HOMOLOGADO'

            WHEN es_producto_prueba = TRUE
              THEN 'PRODUCTO_PRUEBA_NO_HOMOLOGADO'

            WHEN es_producto_homologado = FALSE
                THEN 'PRODUCTO_NO_HOMOLOGADO'

            WHEN es_linea_homologada = FALSE
                THEN 'LINEA_NO_HOMOLOGADA'

            WHEN es_monto_en_rango_producto = FALSE
                THEN 'MONTO_FUERA_DE_RANGO_PRODUCTO'

            ELSE NULL
        END AS observacion_calidad

    FROM indicadores_calidad
)

-- 11. Estructura final de Silver
SELECT
    pais,
    id_transaccion,
    trx_id,
    comercio_id,

    producto_codigo_origen,
    producto_codigo_catalogo,
    nombre_producto,

    canal_codigo_origen,
    canal_homologado,

    linea_negocio_producto,
    linea_negocio_corporativa,

    fecha_hora_negocio,
    fecha_negocio,

    monto,
    comision,
    moneda,

    estado_origen,
    estado,

    referencia,
    ultima_operacion_cdc,
    cdc_lsn,

    archivo_origen,
    ingestion_wave,

    monto_minimo,
    monto_maximo,

    es_estado_valido,
    es_fecha_valida,
    es_monto_valido,
    es_comision_valida,
    es_canal_homologado,
    es_producto_homologado,
    es_producto_prueba,
    es_linea_homologada,
    es_monto_en_rango_producto,

    observacion_calidad,

    CURRENT_TIMESTAMP() AS fecha_proceso_silver

FROM calidad_final;
