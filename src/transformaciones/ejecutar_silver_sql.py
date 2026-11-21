
from pathlib import Path
import sys
import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. RUTAS DEL PROYECTO
# ============================================================

PROYECTO = Path(__file__).resolve().parents[2]

RUTA_BRONZE = PROYECTO / "data" / "lakehouse" / "bronze"

RUTA_CUARENTENA = (
    RUTA_BRONZE / "quarantine" / "mx_transacciones"
)

RUTA_CRM = RUTA_BRONZE / "crm_comercios"

RUTA_SILVER_CRM = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "silver"
    / "crm_comercios_vigente"
)

RUTA_CUARENTENA_CRM = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "silver"
    / "crm_comercios_cuarentena"
)

RUTA_VALIDACION_TRL = (
    RUTA_BRONZE / "quality" / "validacion_trl_mexico"
)

RUTA_SILVER_SQL = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "silver"
    / "transacciones_sql"
)

RUTA_SQL = PROYECTO / "sql" / "silver"


RUTAS_BRONZE = {
    "bronze_co": RUTA_BRONZE / "co_transacciones_cdc",
    "bronze_mx": RUTA_BRONZE / "mx_transacciones",
    "bronze_comercios": RUTA_CRM,
    "catalogo_productos": RUTA_BRONZE / "catalogos_productos",
    "catalogo_canales": RUTA_BRONZE / "catalogos_canales",
    "catalogo_taxonomia": RUTA_BRONZE / "catalogos_taxonomia",
    
}


ARCHIVOS_SQL = [
    "01_transacciones_colombia.sql",
    "02_transacciones_mexico.sql",
    "03_consolidar_transacciones.sql",
    "04_crm_comercios.sql",
]


# ============================================================
# 2. CONFIGURAR SPARK + DELTA
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
    .appName("Silver_SQL_Pulso")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "America/Bogota")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. REGISTRAR TABLAS DE CALIDAD
# ============================================================

def registrar_tabla_calidad(nombre_vista, ruta):

    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe la tabla de calidad: {ruta}"
        )

    if not (ruta / "_delta_log").exists():
        raise ValueError(
            f"La ruta no contiene una tabla Delta válida: {ruta}"
        )

    df = (
        spark.read
        .format("delta")
        .load(str(ruta))
    )

    df.createOrReplaceTempView(nombre_vista)

    print(
        f"Tabla de calidad registrada: {nombre_vista} "
        f"| Registros: {df.count()} "
        f"| Ruta: {ruta}"
    )


registrar_tabla_calidad(
    "quarantine_mx",
    RUTA_CUARENTENA
)

registrar_tabla_calidad(
    "validacion_trl_mexico",
    RUTA_VALIDACION_TRL
)


# ============================================================
# 4. REGISTRAR TABLAS BRONZE COMO VISTAS
# ============================================================

def registrar_delta(nombre_vista, ruta):

    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe la tabla Bronze requerida: {ruta}"
        )

    if not (ruta / "_delta_log").exists():
        raise ValueError(
            f"La ruta no contiene una tabla Delta válida: {ruta}"
        )

    df = (
        spark.read
        .format("delta")
        .load(str(ruta))
    )

    df.createOrReplaceTempView(nombre_vista)

    total = df.count()

    print(
        f"Vista registrada: {nombre_vista} "
        f"| Registros: {total}"
    )


# ============================================================
# 5. EJECUTAR ARCHIVOS SQL
# ============================================================

def ejecutar_sql(nombre_archivo):

    ruta_archivo = RUTA_SQL / nombre_archivo

    if not ruta_archivo.exists():
        raise FileNotFoundError(
            f"No existe el archivo SQL: {ruta_archivo}"
        )

    print(f"\nEjecutando: {nombre_archivo}")

    contenido_sql = ruta_archivo.read_text(
        encoding="utf-8"
    )

    sentencias = [
        sentencia.strip()
        for sentencia in contenido_sql.split(";")
        if sentencia.strip()
    ]

    if not sentencias:
        raise ValueError(
            f"El archivo SQL está vacío: {nombre_archivo}"
        )

    for numero, sentencia in enumerate(sentencias, start=1):

        print(
            f"Ejecutando sentencia "
            f"{numero}/{len(sentencias)}"
        )

        spark.sql(sentencia)

    print(
        f"Finalizado: {nombre_archivo} "
        f"| Sentencias ejecutadas: {len(sentencias)}"
    )


# ============================================================
# 6. PROCESO PRINCIPAL
# ============================================================

try:

    # --------------------------------------------------------
    # REGISTRAR BRONZE
    # --------------------------------------------------------

    print("\n========== REGISTRO DE TABLAS BRONZE ==========")

    for nombre_vista, ruta in RUTAS_BRONZE.items():
        registrar_delta(nombre_vista, ruta)


    # --------------------------------------------------------
    # SQL 01: COLOMBIA
    # --------------------------------------------------------

    print("\n========== TRANSFORMACION COLOMBIA ==========")

    ejecutar_sql(ARCHIVOS_SQL[0])

    df_co = spark.table("transacciones_colombia")

    print("Total Colombia SQL:", df_co.count())


    # --------------------------------------------------------
    # SQL 02: MEXICO
    # --------------------------------------------------------

    print("\n========== TRANSFORMACION MEXICO ==========")

    ejecutar_sql(ARCHIVOS_SQL[1])

    df_mx = spark.table("transacciones_mexico")

    print("Total Mexico SQL:", df_mx.count())

    print("\n========== INVESTIGACION ESTADOS MEXICO ==========")

    spark.sql("""
        SELECT
            ESTATUS,
            COUNT(*) AS cantidad
        FROM bronze_mx
        WHERE UPPER(TRIM(tipo_registro)) = 'TRANSACCION'
        GROUP BY ESTATUS
        ORDER BY cantidad DESC
    """).show(100, truncate=False)

    print("\n========== CANALES COLOMBIA ==========")

    spark.sql("""
        SELECT
            canal_codigo_origen,
            COUNT(*) AS cantidad
        FROM transacciones_colombia
        GROUP BY canal_codigo_origen
        ORDER BY cantidad DESC
    """).show(100, truncate=False)

 

    print("\n========== CANALES MEXICO ==========")

    spark.sql("""
        SELECT
            canal_codigo_origen,
            COUNT(*) AS cantidad
        FROM transacciones_mexico
        GROUP BY canal_codigo_origen
        ORDER BY cantidad DESC
    """).show(100, truncate=False)


    print("\n========== CATALOGO DE CANALES ==========")

    spark.sql("""
        SELECT *
        FROM catalogo_canales
    """).show(100, truncate=False)

    # --------------------------------------------------------
    # VALIDAR DUPLICADOS POR PAIS
    # --------------------------------------------------------

    print("\n========== VALIDACION DE DUPLICADOS ==========")

    duplicados_co = (
        df_co
        .groupBy("pais", "id_transaccion")
        .count()
        .filter("count > 1")
        .count()
    )

    duplicados_mx = (
        df_mx
        .groupBy("pais", "id_transaccion")
        .count()
        .filter("count > 1")
        .count()
    )

    print("Duplicados Colombia:", duplicados_co)
    print("Duplicados Mexico:", duplicados_mx)

    if duplicados_co > 0 or duplicados_mx > 0:
        raise ValueError(
            "Se encontraron duplicados antes de consolidar Silver."
        )


    # --------------------------------------------------------
    # SQL 03: CONSOLIDACION Y HOMOLOGACION
    # --------------------------------------------------------

    print("\n========== CONSOLIDACION DE TRANSACCIONES ==========")

    ejecutar_sql(ARCHIVOS_SQL[2])

    df_silver = spark.table("silver_consolidada")

    # --------------------------------------------------------
    # VALIDAR PRODUCTOS DE PRUEBA TST-001
    # --------------------------------------------------------

    print("\n========== VALIDACION PRODUCTOS DE PRUEBA ==========")

    spark.sql("""
        SELECT
            COUNT(*) AS total_registros,
            SUM(
                CASE
                    WHEN es_producto_prueba = TRUE THEN 1
                    ELSE 0
                END
            ) AS registros_identificados,
            SUM(
                CASE
                    WHEN linea_negocio_corporativa IS NOT NULL THEN 1
                    ELSE 0
                END
            ) AS registros_con_linea_corporativa
        FROM silver_consolidada
        WHERE pais = 'CO'
          AND UPPER(TRIM(producto_codigo_origen)) = 'TST-001'
    """).show(truncate=False)

    print("\n========== DETALLE PRODUCTOS DE PRUEBA ==========")

    spark.sql("""
        SELECT
            pais,
            producto_codigo_origen,
            es_producto_prueba,
            linea_negocio_corporativa,
            es_producto_homologado,
            es_linea_homologada,
            observacion_calidad
        FROM silver_consolidada
        WHERE pais = 'CO'
          AND UPPER(TRIM(producto_codigo_origen)) = 'TST-001'
        LIMIT 20
    """).show(truncate=False)

    # ==========================================================
    # DIAGNOSTICO DEL CATALOGO DE PRODUCTOS
    # ==========================================================

    print("\n========== PRODUCTOS POR PAIS Y LINEA DE NEGOCIO ==========")

    spark.sql("""
        SELECT
            UPPER(TRIM(pais)) AS pais,
            TRIM(linea_negocio) AS linea_negocio,
            COUNT(*) AS cantidad_productos,
            SUM(CASE WHEN activo = TRUE THEN 1 ELSE 0 END) AS productos_activos,
            SUM(CASE WHEN activo = FALSE THEN 1 ELSE 0 END) AS productos_inactivos
        FROM catalogo_productos
        GROUP BY
            UPPER(TRIM(pais)),
            TRIM(linea_negocio)
        ORDER BY pais, linea_negocio
    """).show(100, truncate=False)


    print("\n========== PRODUCTOS Y VIGENCIAS ==========")

    spark.sql("""
        SELECT
            pais,
            producto_cod,
            sku_origen,
            nombre,
            linea_negocio,
            activo,
            vigente_desde,
            vigente_hasta,
            limites.monto_min AS monto_minimo,
            limites.monto_max AS monto_maximo
        FROM catalogo_productos
        ORDER BY pais, linea_negocio, producto_cod
    """).show(100, truncate=False)


    print("\n========== TAXONOMIA CORPORATIVA ==========")

    spark.sql("""
        SELECT
            linea_negocio_corporativa,
            descripcion
        FROM catalogo_taxonomia
        ORDER BY linea_negocio_corporativa
    """).show(100, truncate=False)


    print("\n========== LINEAS DE MEXICO SIN HOMOLOGACION ==========")

    spark.sql("""
        SELECT
            linea_negocio_producto,
            COUNT(*) AS cantidad_transacciones,
            COUNT(DISTINCT producto_codigo_origen) AS productos_distintos
        FROM silver_consolidada
        WHERE pais = 'MX'
        AND es_linea_homologada = FALSE
        GROUP BY linea_negocio_producto
        ORDER BY cantidad_transacciones DESC
    """).show(100, truncate=False)


    print("\n========== PRODUCTOS MEXICANOS SIN HOMOLOGACION ==========")

    spark.sql("""
        SELECT
            producto_codigo_origen,
            producto_codigo_catalogo,
            linea_negocio_producto,
            COUNT(*) AS cantidad_transacciones
        FROM silver_consolidada
        WHERE pais = 'MX'
        AND es_producto_homologado = FALSE
        GROUP BY
            producto_codigo_origen,
            producto_codigo_catalogo,
            linea_negocio_producto
        ORDER BY cantidad_transacciones DESC
    """).show(100, truncate=False)


    print("\n========== DIAGNOSTICO JOIN PRODUCTOS MEXICO ==========")

    spark.sql("""
        SELECT
            t.pais,
            t.producto_codigo_origen,
            p.sku_origen,
            p.producto_cod,
            t.fecha_negocio,
            p.vigente_desde,
            p.vigente_hasta,

            CASE
                WHEN p.sku_origen IS NULL
                    THEN 'NO COINCIDE SKU'
                WHEN t.fecha_negocio IS NULL
                    THEN 'FECHA NEGOCIO NULA'
                WHEN t.fecha_negocio < p.vigente_desde
                    THEN 'FUERA DE VIGENCIA INICIAL'
                WHEN p.vigente_hasta IS NOT NULL
                    AND t.fecha_negocio > p.vigente_hasta
                    THEN 'FUERA DE VIGENCIA FINAL'
                ELSE 'COINCIDENCIA'
            END AS resultado_diagnostico

        FROM transacciones_mexico t

        LEFT JOIN (
            SELECT
                UPPER(TRIM(pais)) AS pais,
                TRIM(sku_origen) AS sku_origen,
                TRIM(producto_cod) AS producto_cod,
                TO_DATE(vigente_desde) AS vigente_desde,
                TO_DATE(vigente_hasta) AS vigente_hasta
            FROM catalogo_productos
        ) p
            ON UPPER(TRIM(t.pais)) = p.pais
            AND TRIM(CAST(t.producto_codigo_origen AS STRING))
                = p.sku_origen

        WHERE t.producto_codigo_origen IS NOT NULL

    """).groupBy(
        "resultado_diagnostico"
    ).count().show(truncate=False)


    print("\n========== PRUEBA DE COINCIDENCIA SIN VIGENCIA ==========")

    spark.sql("""
        SELECT
            t.producto_codigo_origen,
            p.producto_cod,
            p.sku_origen,
            p.linea_negocio,
            COUNT(*) AS cantidad

        FROM transacciones_mexico t

        LEFT JOIN catalogo_productos p
            ON UPPER(TRIM(t.pais)) = UPPER(TRIM(p.pais))
            AND TRIM(CAST(t.producto_codigo_origen AS STRING))
                = TRIM(CAST(p.sku_origen AS STRING))

        GROUP BY
            t.producto_codigo_origen,
            p.producto_cod,
            p.sku_origen,
            p.linea_negocio

        ORDER BY cantidad DESC

    """).show(100, truncate=False)

    print("\n========== DETALLE DE PRODUCTOS MEXICO SIN HOMOLOGACION ==========")

    spark.sql("""
        SELECT
            t.producto_codigo_origen,
            COUNT(*) AS cantidad,
            COUNT(DISTINCT t.id_transaccion) AS transacciones_unicas,
            SUM(
                CASE WHEN t.fecha_negocio IS NULL THEN 1 ELSE 0 END
            ) AS fechas_nulas,
            MIN(t.fecha_negocio) AS fecha_minima,
            MAX(t.fecha_negocio) AS fecha_maxima
        FROM transacciones_mexico t
        LEFT JOIN (
            SELECT
                UPPER(TRIM(pais)) AS pais,
                TRIM(CAST(sku_origen AS STRING)) AS sku_origen,
                TO_DATE(vigente_desde) AS vigente_desde,
                TO_DATE(vigente_hasta) AS vigente_hasta
            FROM catalogo_productos
        ) p
        ON UPPER(TRIM(t.pais)) = p.pais
        AND TRIM(CAST(t.producto_codigo_origen AS STRING)) = p.sku_origen
        AND t.fecha_negocio >= p.vigente_desde
        AND (
            p.vigente_hasta IS NULL
            OR t.fecha_negocio <= p.vigente_hasta
        )
        WHERE UPPER(TRIM(t.pais)) = 'MX'
        AND p.sku_origen IS NULL
        GROUP BY t.producto_codigo_origen
        ORDER BY cantidad DESC
    """).show(100, truncate=False)

    print("\n========== VERIFICA DISTRIBUCION  ==========")
    spark.sql("""
            SELECT
                pais,
                estado,
                COUNT(*) AS cantidad
            FROM silver_consolidada
            GROUP BY pais, estado
            ORDER BY pais, estado
        """).show(100, truncate=False)

    print("\n========== VALIDA NULOS ==========")
    spark.sql("""
            SELECT
                pais,
                COUNT(*) AS estados_nulos
            FROM silver_consolidada
            WHERE estado IS NULL
            GROUP BY pais
        """).show(100, truncate=False)
    # --------------------------------------------------------
    # VALIDACIONES SILVER FINAL
    # --------------------------------------------------------

    print("\n========== VALIDACIONES SILVER FINAL ==========")

    total_silver = df_silver.count()

    duplicados_silver = (
        df_silver
        .groupBy("pais", "id_transaccion")
        .count()
        .filter("count > 1")
        .count()
    )

    llaves_nulas = df_silver.filter(
        """
        pais IS NULL
        OR id_transaccion IS NULL
        """
    ).count()

    print("Total transacciones:", total_silver)
    print("Duplicados Silver:", duplicados_silver)
    print("Llaves nulas:", llaves_nulas)

    if duplicados_silver > 0:
        raise ValueError(
            "La tabla Silver final contiene llaves duplicadas."
        )

    if llaves_nulas > 0:
        raise ValueError(
            "La tabla Silver final contiene llaves nulas."
        )


# ==========================================================
# VALIDACION DE HOMOLOGACION DE CANALES
# ==========================================================

    print("\n========== CANALES HOMOLOGADOS EN SILVER ==========")

    spark.sql("""
        SELECT
            pais,
            canal_codigo_origen,
            canal_homologado,
            COUNT(*) AS cantidad
        FROM silver_consolidada
        GROUP BY
            pais,
            canal_codigo_origen,
            canal_homologado
        ORDER BY
            pais,
            canal_codigo_origen
    """).show(100, truncate=False)


    print("\n========== CANALES SIN HOMOLOGAR ==========")

    spark.sql("""
        SELECT
            pais,
            COUNT(*) AS cantidad_sin_homologar
        FROM silver_consolidada
        WHERE canal_homologado IS NULL
        GROUP BY pais
        ORDER BY pais
    """).show(100, truncate=False)


    print("\n========== RESUMEN DE HOMOLOGACION ==========")

    spark.sql("""
        SELECT
            pais,
            COUNT(*) AS total_transacciones,
            COUNT(canal_homologado) AS canales_homologados,
            COUNT(*) - COUNT(canal_homologado) AS canales_pendientes
        FROM silver_consolidada
        GROUP BY pais
        ORDER BY pais
    """).show(truncate=False)

    # ========================================================
    # SQL 04: TRANSFORMACION CRM
    # ========================================================

    print("\n========== TRANSFORMACION CRM ==========")

    ejecutar_sql(ARCHIVOS_SQL[3])

    # --------------------------------------------------------
    # OBTENER VISTAS CRM
    # --------------------------------------------------------

    df_crm = spark.table("silver_crm_comercios")

    df_crm_cuarentena = spark.table(
        "crm_comercios_cuarentena"
    )

    # --------------------------------------------------------
    # VALIDACIONES DE CALIDAD CRM
    # --------------------------------------------------------

    print("\n========== VALIDACIONES SILVER CRM ==========")

    total_crm = df_crm.count()

    total_cuarentena_crm = df_crm_cuarentena.count()

    duplicados_crm = (
        df_crm
        .groupBy("pais", "comercio_id")
        .count()
        .filter("count > 1")
        .count()
    )

    llaves_nulas_crm = df_crm.filter(
        """
        pais IS NULL
        OR comercio_id IS NULL
        """
    ).count()

    print("Comercios vigentes:", total_crm)
    print("Registros en cuarentena:", total_cuarentena_crm)
    print("Duplicados CRM:", duplicados_crm)
    print("Llaves nulas CRM:", llaves_nulas_crm)

    if duplicados_crm > 0:
        raise ValueError(
            "Silver CRM contiene llaves duplicadas."
        )

    if llaves_nulas_crm > 0:
        raise ValueError(
            "Silver CRM contiene llaves nulas."
        )

    # --------------------------------------------------------
    # DISTRIBUCION DE COMERCIOS POR PAIS
    # --------------------------------------------------------

    print("\n========== COMERCIOS VIGENTES POR PAIS ==========")

    df_crm.groupBy(
        "pais"
    ).count().orderBy(
        "pais"
    ).show(100, truncate=False)

    # --------------------------------------------------------
    # DISTRIBUCION DE ESTADOS
    # --------------------------------------------------------

    print("\n========== ESTADOS DE COMERCIOS ==========")

    df_crm.groupBy(
        "pais",
        "estado"
    ).count().orderBy(
        "pais",
        "estado"
    ).show(100, truncate=False)

    # --------------------------------------------------------
    # CUARENTENA POR MOTIVO
    # --------------------------------------------------------

    print("\n========== CUARENTENA CRM POR MOTIVO ==========")

    if total_cuarentena_crm > 0:

        df_crm_cuarentena.groupBy(
            "observacion_calidad"
        ).count().orderBy(
            "observacion_calidad"
        ).show(100, truncate=False)

    else:
        print("No existen registros en cuarentena.")

    # --------------------------------------------------------
    # COMERCIOS POR SNAPSHOT
    # --------------------------------------------------------

    print("\n========== COMERCIOS POR SNAPSHOT ==========")

    df_crm.groupBy(
        "snapshot_date"
    ).count().orderBy(
        "snapshot_date"
    ).show(100, truncate=False)

    # --------------------------------------------------------
    # VALIDAR FECHA MAXIMA DEL MAESTRO
    # --------------------------------------------------------

    print("\n========== ULTIMO SNAPSHOT CRM ==========")

    spark.sql("""
        SELECT
            MAX(snapshot_date) AS ultimo_snapshot,
            COUNT(*) AS total_comercios
        FROM silver_crm_comercios
    """).show(truncate=False)

    # --------------------------------------------------------
    # ESCRITURA DELTA: MAESTRO CRM
    # --------------------------------------------------------

    print("\n========== ESCRITURA SILVER CRM ==========")

    (
        df_crm.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(RUTA_SILVER_CRM))
    )

    # --------------------------------------------------------
    # ESCRITURA DELTA: CUARENTENA CRM
    # --------------------------------------------------------

    print("\n========== ESCRITURA CUARENTENA CRM ==========")

    (
        df_crm_cuarentena.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(RUTA_CUARENTENA_CRM))
    )

    # --------------------------------------------------------
    # VALIDAR ESCRITURA DELTA
    # --------------------------------------------------------

    df_crm_guardado = (
        spark.read
        .format("delta")
        .load(str(RUTA_SILVER_CRM))
    )

    df_cuarentena_guardada = (
        spark.read
        .format("delta")
        .load(str(RUTA_CUARENTENA_CRM))
    )

    total_crm_guardado = df_crm_guardado.count()

    total_cuarentena_guardado = (
        df_cuarentena_guardada.count()
    )

    print("\n========== RESULTADO FINAL SILVER CRM ==========")

    print("Comercios generados:", total_crm)
    print("Comercios almacenados:", total_crm_guardado)

    print("Cuarentena generada:", total_cuarentena_crm)
    print("Cuarentena almacenada:", total_cuarentena_guardado)

    if total_crm != total_crm_guardado:
        raise ValueError(
            "La cantidad de comercios almacenados "
            "no coincide con la cantidad generada."
        )

    if total_cuarentena_crm != total_cuarentena_guardado:
        raise ValueError(
            "La cantidad de registros de cuarentena "
            "almacenados no coincide con la cantidad generada."
        )

    print("\nEsquema Silver CRM:")
    df_crm_guardado.printSchema()

    print("\nRuta maestro CRM:")
    print(RUTA_SILVER_CRM)

    print("\nRuta cuarentena CRM:")
    print(RUTA_CUARENTENA_CRM)

    print("\nTransformación CRM finalizada correctamente.")

    # --------------------------------------------------------
    # RESULTADOS POR PAIS
    # --------------------------------------------------------

    print("\nTransacciones por pais:")

    df_silver.groupBy("pais").count().show()


    # --------------------------------------------------------
    # DISTRIBUCION DE CALIDAD
    # --------------------------------------------------------

    print("\n========== RESULTADO DE CALIDAD ==========")

    df_silver.groupBy(
        "observacion_calidad"
    ).count().orderBy(
        "observacion_calidad"
    ).show(100, False)


    # --------------------------------------------------------
    # PRODUCTOS NO HOMOLOGADOS
    # --------------------------------------------------------

    print("\nProductos no homologados:")

    df_silver.filter(
        "es_producto_homologado = false"
    ).groupBy(
        "pais",
        "producto_codigo_origen"
    ).count().orderBy(
        "pais",
        "producto_codigo_origen"
    ).show(100, False)


    # --------------------------------------------------------
    # LINEAS CORPORATIVAS NO HOMOLOGADAS
    # --------------------------------------------------------

    print("\nLineas corporativas no homologadas:")

    df_silver.filter(
        "es_linea_homologada = false"
    ).groupBy(
        "pais",
        "linea_negocio_producto",
        "linea_negocio_corporativa"
    ).count().show(100, False)


    # --------------------------------------------------------
    # ESCRITURA DELTA
    # --------------------------------------------------------

    print("\n========== ESCRITURA SILVER SQL ==========")

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(RUTA_SILVER_SQL))
    )


    # --------------------------------------------------------
    # VALIDAR ESCRITURA
    # --------------------------------------------------------

    df_guardado = (
        spark.read
        .format("delta")
        .load(str(RUTA_SILVER_SQL))
    )

    total_guardado = df_guardado.count()

    print("\n========== RESULTADO FINAL SILVER SQL ==========")

    print("Registros generados:", total_silver)
    print("Registros almacenados:", total_guardado)

    if total_silver != total_guardado:
        raise ValueError(
            "La cantidad escrita no coincide con la cantidad transformada."
        )

    print("\nEsquema final:")
    df_guardado.printSchema()

    print("\nRuta de almacenamiento:")
    print(RUTA_SILVER_SQL)

    print("\nProceso Silver SQL finalizado correctamente.")

finally:
    spark.stop()
