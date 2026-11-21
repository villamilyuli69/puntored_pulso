
CREATE OR REPLACE TEMP VIEW transacciones_mexico AS

WITH archivos_bloqueados AS (

    -- 1. Identificar archivos que no pueden publicarse en Silver
    SELECT DISTINCT
        source_file
    FROM validacion_trl_mexico
    WHERE estado_publicacion = 'BLOQUEADO'
),

transacciones_origen AS (
    -- 2. Obtener únicamente los registros de transacciones
    SELECT
        *,
        COALESCE(
            TO_TIMESTAMP(
                TRIM(FECHA_HORA),
                'dd/MM/yyyy HH:mm:ss'
            ),
            TO_TIMESTAMP(
                TRIM(FECHA_HORA),
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            )
        ) AS fecha_hora_negocio,

        COALESCE(
            TO_TIMESTAMP(
                TRIM(FECHA_ACT),
                'dd/MM/yyyy HH:mm:ss'
            ),
            TO_TIMESTAMP(
                TRIM(FECHA_ACT),
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            )
        ) AS fecha_actualizacion
    FROM bronze_mx
    WHERE UPPER(TRIM(tipo_registro)) = 'TRANSACCION'
      AND ID_OPERACION IS NOT NULL
      AND TRIM(ID_OPERACION) <> ''
),

transacciones_ordenadas AS (
    -- 3. Obtener la última versión de cada operación
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(ID_OPERACION)
            ORDER BY
                fecha_actualizacion DESC NULLS LAST,
                fecha_carga_archivo DESC NULLS LAST,
                ingestion_timestamp DESC NULLS LAST,
                source_file DESC NULLS LAST
        ) AS rn_operacion

    FROM transacciones_origen
),

transacciones_vigentes AS (
    SELECT *
    FROM transacciones_ordenadas
    WHERE rn_operacion = 1
),

transacciones_sin_cuarentena AS (
    -- 4. Excluir únicamente las transacciones identificadas
    -- en la tabla de cuarentena.
    -- Se utiliza el archivo y el ID para identificar el registro.

    SELECT v.*
    FROM transacciones_vigentes v
    LEFT ANTI JOIN quarantine_mx q
        ON v.source_file = q.source_file
       AND TRIM(CAST(v.ID_OPERACION AS STRING))
            = TRIM(CAST(q.ID_OPERACION AS STRING))
),

transacciones_publicables AS (
    -- 5. Excluir todos los registros pertenecientes
    -- a archivos bloqueados por la validación TRL.
    SELECT t.*
    FROM transacciones_sin_cuarentena t
    LEFT ANTI JOIN archivos_bloqueados b
        ON t.source_file = b.source_file
),

resultado AS (
    -- 6. Estandarización de los campos de México
    SELECT
        'MX' AS pais,
        TRIM(ID_OPERACION)
            AS id_transaccion,
        CAST(NULL AS BIGINT)
            AS trx_id,
        TRIM(ID_COMERCIO)
            AS comercio_id,
        TRIM(SKU)
            AS producto_codigo_origen,
        UPPER(TRIM(CANAL))
            AS canal_codigo_origen,
        CAST(
            REGEXP_REPLACE(
                TRIM(CAST(MONTO AS STRING)),
                '[$,]',
                ''
            ) AS DECIMAL(18,2)
        ) AS monto,
        CAST(
            REGEXP_REPLACE(
                TRIM(CAST(COMISION AS STRING)),
                '[$,]',
                ''
            ) AS DECIMAL(18,2)
        ) AS comision,

        'MXN' AS moneda,
        UPPER(TRIM(ESTATUS))
            AS estado_origen,
        CASE
            WHEN UPPER(TRIM(ESTATUS)) = 'CANCELADA'
                THEN 'REVERSADA'

            WHEN LOWER(TRIM(ESTATUS)) = 'approved'
                THEN 'APROBADA'

            WHEN LOWER(TRIM(ESTATUS)) = 'declined'
                THEN 'RECHAZADA'

            WHEN LOWER(TRIM(ESTATUS)) = 'pending'
                THEN 'PENDIENTE'

            WHEN UPPER(TRIM(ESTATUS)) IN (
                'APROBADA',
                'RECHAZADA',
                'PENDIENTE'
            )
                THEN UPPER(TRIM(ESTATUS))
            ELSE NULL
        END AS estado,
        fecha_hora_negocio,
        TO_DATE(fecha_hora_negocio)
            AS fecha_negocio,
        TRIM(REFERENCIA)
            AS referencia,
        CAST(NULL AS STRING)
            AS ultima_operacion_cdc,
        CAST(NULL AS BIGINT)
            AS cdc_lsn,
        source_file AS archivo_origen,
        ingestion_wave
    FROM transacciones_publicables
)

SELECT *
FROM resultado;
