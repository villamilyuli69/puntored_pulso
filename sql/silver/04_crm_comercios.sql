
-- ============================================================
-- SILVER CRM: NORMALIZACION, CALIDAD Y VERSION VIGENTE
-- Origen: bronze_comercios
-- ============================================================


-- ============================================================
-- 1. NORMALIZAR CAMPOS Y CONVERTIR TIPOS DE DATOS
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_normalizado AS

SELECT
    TRY_CAST(NULLIF(TRIM(comercio_id), '') AS BIGINT) AS comercio_id,

    UPPER(NULLIF(TRIM(pais), '')) AS pais,

    NULLIF(TRIM(nombre_comercial), '') AS nombre_comercial,

    UPPER(NULLIF(TRIM(documento_tributario), '')) AS documento_tributario,

    NULLIF(TRIM(segmento), '') AS segmento,

    NULLIF(TRIM(ciudad), '') AS ciudad,

    NULLIF(TRIM(region), '') AS region,

    UPPER(NULLIF(TRIM(estado), '')) AS estado,

    UPPER(NULLIF(TRIM(plan_comision), '')) AS plan_comision,

    COALESCE(
        TO_DATE(NULLIF(TRIM(fecha_afiliacion), ''), 'yyyy-MM-dd'),
        TO_DATE(NULLIF(TRIM(fecha_afiliacion), ''), 'yyyyMMdd'),
        TO_DATE(NULLIF(TRIM(fecha_afiliacion), ''), 'dd/MM/yyyy')
    ) AS fecha_afiliacion,

    TO_TIMESTAMP(
        NULLIF(TRIM(actualizado_en), ''),
        'yyyy-MM-dd HH:mm:ss'
    ) AS actualizado_en,

    TO_DATE(
        NULLIF(TRIM(snapshot_date), ''),
        'yyyy-MM-dd'
    ) AS snapshot_date,

    source_file,
    ingestion_timestamp,
    ingestion_wave

FROM bronze_comercios;


-- ============================================================
-- 2. CLASIFICAR REGISTROS SEGUN REGLAS DE CALIDAD
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_clasificacion_calidad AS

SELECT
    *,

    CASE
        WHEN comercio_id IS NULL
            THEN 'COMERCIO_ID_INVALIDO'

        WHEN pais IS NULL OR pais NOT IN ('CO', 'MX')
            THEN 'PAIS_INVALIDO'

        WHEN nombre_comercial IS NULL
            THEN 'NOMBRE_COMERCIAL_VACIO'

        WHEN documento_tributario IS NULL
            THEN 'DOCUMENTO_TRIBUTARIO_VACIO'

        WHEN estado IS NULL
            OR estado NOT IN ('ACTIVO', 'INACTIVO', 'SUSPENDIDO')
            THEN 'ESTADO_INVALIDO'

        WHEN fecha_afiliacion IS NULL
            THEN 'FECHA_AFILIACION_INVALIDA'

        WHEN actualizado_en IS NULL
            THEN 'FECHA_ACTUALIZACION_INVALIDA'

        WHEN snapshot_date IS NULL
            THEN 'SNAPSHOT_DATE_INVALIDO'

        ELSE NULL
    END AS observacion_calidad

FROM crm_normalizado;


-- ============================================================
-- 3. SEPARAR REGISTROS QUE CUMPLEN LAS REGLAS
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_registros_validos AS

SELECT *
FROM crm_clasificacion_calidad
WHERE observacion_calidad IS NULL;


-- ============================================================
-- 4. IDENTIFICAR DUPLICADOS DENTRO DE CADA SNAPSHOT
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_duplicados_snapshot AS

SELECT
    *,

    ROW_NUMBER() OVER (
        PARTITION BY
            pais,
            comercio_id,
            snapshot_date
        ORDER BY
            actualizado_en DESC NULLS LAST,
            ingestion_timestamp DESC NULLS LAST,
            source_file DESC NULLS LAST
    ) AS rn_snapshot

FROM crm_registros_validos;


-- ============================================================
-- 5. CONSTRUIR LA VISTA DE CUARENTENA
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_comercios_cuarentena AS

-- Registros que incumplen reglas de calidad
SELECT
    comercio_id,
    pais,
    nombre_comercial,
    documento_tributario,
    segmento,
    ciudad,
    region,
    estado,
    plan_comision,
    fecha_afiliacion,
    actualizado_en,
    snapshot_date,
    source_file,
    ingestion_timestamp,
    ingestion_wave,
    observacion_calidad,
    CURRENT_TIMESTAMP() AS fecha_proceso_silver

FROM crm_clasificacion_calidad

WHERE observacion_calidad IS NOT NULL

UNION ALL

-- Registros duplicados dentro del mismo snapshot
SELECT
    comercio_id,
    pais,
    nombre_comercial,
    documento_tributario,
    segmento,
    ciudad,
    region,
    estado,
    plan_comision,
    fecha_afiliacion,
    actualizado_en,
    snapshot_date,
    source_file,
    ingestion_timestamp,
    ingestion_wave,
    'DUPLICADO_EN_SNAPSHOT' AS observacion_calidad,
    CURRENT_TIMESTAMP() AS fecha_proceso_silver

FROM crm_duplicados_snapshot

WHERE rn_snapshot > 1;


-- ============================================================
-- 6. CONSERVAR UNA FILA POR COMERCIO EN CADA SNAPSHOT
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_comercios_sin_duplicados AS

SELECT *
FROM crm_duplicados_snapshot
WHERE rn_snapshot = 1;


-- ============================================================
-- 7. SELECCIONAR LA ULTIMA VERSION DE CADA COMERCIO
-- ============================================================

CREATE OR REPLACE TEMP VIEW crm_versiones_ordenadas AS

SELECT
    *,

    ROW_NUMBER() OVER (
        PARTITION BY
            pais,
            comercio_id
        ORDER BY
            snapshot_date DESC,
            actualizado_en DESC NULLS LAST,
            ingestion_timestamp DESC NULLS LAST,
            source_file DESC NULLS LAST
    ) AS rn_vigente

FROM crm_comercios_sin_duplicados;


-- ============================================================
-- 8. RESULTADO FINAL DE SILVER CRM
-- ============================================================

CREATE OR REPLACE TEMP VIEW silver_crm_comercios AS

SELECT
    pais,
    comercio_id,
    nombre_comercial,
    documento_tributario,
    segmento,
    ciudad,
    region,
    estado,
    plan_comision,
    fecha_afiliacion,
    actualizado_en,
    snapshot_date,
    source_file,
    ingestion_timestamp,
    ingestion_wave,

    TRUE AS es_version_actual,

    CURRENT_TIMESTAMP() AS fecha_proceso_silver

FROM crm_versiones_ordenadas

WHERE rn_vigente = 1;
