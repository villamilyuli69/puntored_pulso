
CREATE OR REPLACE TEMP VIEW transacciones_colombia AS

WITH eventos_validos AS (
    SELECT
        *,
        COALESCE(
            value.after.trx_id,
            value.before.trx_id,
            `key`.trx_id
        ) AS id_transaccion_cdc
    FROM bronze_co
    WHERE value IS NOT NULL
      AND value.op IN ('c', 'u', 'd')
),

-- El mismo mensaje Kafka puede aparecer repetido.
-- Se conserva una sola copia por identidad del mensaje.
eventos_kafka AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY topic, partition, offset
            ORDER BY
                kafka_ts DESC NULLS LAST,
                ingestion_timestamp DESC NULLS LAST
        ) AS rn_kafka
    FROM eventos_validos
),

eventos_sin_duplicados AS (
    SELECT *
    FROM eventos_kafka
    WHERE rn_kafka = 1
),

-- Se determina el último evento de cada transacción.
eventos_ordenados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_transaccion_cdc
            ORDER BY
                value.source.lsn DESC NULLS LAST,
                value.ts_ms DESC NULLS LAST,
                kafka_ts DESC NULLS LAST,
                offset DESC NULLS LAST
        ) AS rn_estado
    FROM eventos_sin_duplicados
    WHERE id_transaccion_cdc IS NOT NULL
),

-- Se excluyen las transacciones eliminadas.
transacciones_vigentes AS (
    SELECT *
    FROM eventos_ordenados
    WHERE rn_estado = 1
      AND value.op <> 'd'
      AND value.after IS NOT NULL
),

-- Estandarización de los campos de Colombia.
resultado AS (
    SELECT
        'CO' AS pais,

        CAST(id_transaccion_cdc AS STRING)
            AS id_transaccion,

        CAST(value.after.trx_id AS BIGINT)
            AS trx_id,

        CAST(value.after.comercio_id AS STRING)
            AS comercio_id,

        TRIM(value.after.producto_cod)
            AS producto_codigo_origen,

        UPPER(TRIM(CAST(value.after.canal_cod AS STRING)))
            AS canal_codigo_origen,

        CAST(value.after.valor AS DECIMAL(18,2))
            AS monto,

        CAST(value.after.comision AS DECIMAL(18,2))
            AS comision,

        UPPER(TRIM(value.after.moneda))
            AS moneda,

        UPPER(TRIM(value.after.estado))
            AS estado_origen,

        CASE
            WHEN UPPER(TRIM(value.after.estado))
                IN ('APROBADA', 'RECHAZADA', 'PENDIENTE', 'REVERSADA')
            THEN UPPER(TRIM(value.after.estado))
            ELSE NULL
        END AS estado,

        CASE
            WHEN CAST(value.after.fecha_trx AS STRING) RLIKE '^[0-9]{14}$'
            THEN TO_TIMESTAMP(
                CAST(value.after.fecha_trx AS STRING),
                'yyyyMMddHHmmss'
            )
            ELSE NULL
        END AS fecha_hora_negocio,

        CASE
            WHEN CAST(value.after.fecha_trx AS STRING) RLIKE '^[0-9]{14}$'
            THEN TO_DATE(
                TO_TIMESTAMP(
                    CAST(value.after.fecha_trx AS STRING),
                    'yyyyMMddHHmmss'
                )
            )
            ELSE NULL
        END AS fecha_negocio,

        CAST(value.after.referencia AS STRING)
            AS referencia,

        value.op AS ultima_operacion_cdc,

        CAST(value.source.lsn AS BIGINT)
            AS cdc_lsn,

        source_file AS archivo_origen,

        ingestion_wave

    FROM transacciones_vigentes
)

SELECT *
FROM resultado;
