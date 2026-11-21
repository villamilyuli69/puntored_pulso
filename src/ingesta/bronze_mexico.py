
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    input_file_name,
    regexp_extract,
    regexp_replace,
    when,
    trim,
    count,
    sum as spark_sum,
    abs as spark_abs
)
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

OLA = sys.argv[1].lower().strip() if len(sys.argv) > 1 else None

OLAS_VALIDAS = ["ola_1", "ola_2"]

if OLA not in OLAS_VALIDAS:
    raise ValueError(
        "Debes indicar una ola válida: ola_1 u ola_2"
    )

PROYECTO = Path(__file__).resolve().parents[2]

RUTA_ORIGEN = (
    PROYECTO
    / "dataset_pulso_ESENCIAL-2026"
    / OLA
    / "landing"
    / "mx"
    / "plataforma_transacciones"
)

RUTA_BRONZE = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "bronze"
    / "mx_transacciones"
)

if not RUTA_ORIGEN.exists():
    raise FileNotFoundError(
        f"No existe la ruta de origen: {RUTA_ORIGEN}"
    )


# ============================================================
# 2. CREACIÓN DE SPARK SESSION CON DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName(f"Bronze_Mexico_{OLA}")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


try:

    # ========================================================
    # 3. LECTURA DE ARCHIVOS CSV
    # ========================================================

    print("=" * 60)
    print(f"INICIANDO INGESTA BRONZE MÉXICO - {OLA.upper()}")
    print("=" * 60)

    print(f"Ruta origen: {RUTA_ORIGEN}")
    print(f"Ruta Bronze: {RUTA_BRONZE}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true")
        .load(str(RUTA_ORIGEN))
    )

    print("Lectura de archivos completada.")

    # ========================================================
    # 4. CLASIFICACIÓN DE REGISTROS
    # ========================================================

    df = (
        df
        .withColumn(
            "tipo_registro",
            when(
                trim(col("ID_OPERACION")) == "TRL",
                "CONTROL"
            ).otherwise("TRANSACCION")
        )
        .withColumn(
            "trl_conteo",
            when(
                col("tipo_registro") == "CONTROL",
                col("FOLIO")
            ).otherwise(lit(None).cast("string"))
        )
        .withColumn(
            "trl_suma_monto",
            when(
                col("tipo_registro") == "CONTROL",
                col("ID_COMERCIO")
            ).otherwise(lit(None).cast("string"))
        )
    )

    # ========================================================
    # 5. METADATOS DE INGESTA
    # ========================================================

    df = (
        df
        .withColumn(
            "source_file",
            input_file_name()
        )
        .withColumn(
            "fecha_carga_archivo",
            regexp_extract(
                col("source_file"),
                r"fecha_carga=(\d{4}-\d{2}-\d{2})",
                1
            )
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "ingestion_wave",
            lit(OLA)
        )
    )

    print("Total de registros leídos:", df.count())

    print("Distribución de registros leídos:")
    df.groupBy("tipo_registro").count().show()

    # ========================================================
    # 6. CONTROL DE INGESTA INCREMENTAL
    # ========================================================

    if RUTA_BRONZE.exists():

        print("La tabla Bronze ya existe. Validando archivos...")

        df_bronze_existente = (
            spark.read
            .format("delta")
            .load(str(RUTA_BRONZE))
        )

        archivos_existentes = (
            df_bronze_existente
            .select("source_file")
            .distinct()
        )

        df_nuevos = (
            df
            .join(
                archivos_existentes,
                on="source_file",
                how="left_anti"
            )
        )

    else:

        print("La tabla Bronze no existe. Se realizará la carga inicial.")

        df_nuevos = df

    cantidad_nuevos = df_nuevos.count()

    cantidad_archivos_nuevos = (
        df_nuevos
        .select("source_file")
        .distinct()
        .count()
    )

    print("Archivos nuevos:", cantidad_archivos_nuevos)
    print("Registros nuevos:", cantidad_nuevos)

    # ========================================================
    # 7. ESCRITURA EN BRONZE DELTA
    # ========================================================

    if cantidad_nuevos > 0:

        print("Iniciando escritura incremental en Bronze...")

        (
            df_nuevos.write
            .format("delta")
            .mode("append")
            .save(str(RUTA_BRONZE))
        )

        print("Escritura Delta completada.")

    else:

        print(
            "No hay archivos nuevos. "
            "No se insertaron registros."
        )

    # ========================================================
    # 8. VALIDACIÓN DE LA INFORMACIÓN ALMACENADA
    # ========================================================

    print("=" * 60)
    print("VALIDACIÓN DE INFORMACIÓN BRONZE")
    print("=" * 60)

    df_bronze = (
        spark.read
        .format("delta")
        .load(str(RUTA_BRONZE))
    )

    print(
        "Total de registros almacenados:",
        df_bronze.count()
    )

    print("Distribución de registros almacenados:")

    df_bronze.groupBy(
        "tipo_registro"
    ).count().show()

    print("Estructura de Bronze:")
    df_bronze.printSchema()

    # ========================================================
    # 9. VALIDACIÓN DE TRL POR ARCHIVO
    # ========================================================
    # Bronze conserva los valores originales.
    # Las conversiones siguientes son temporales y solo
    # se utilizan para las validaciones de calidad.

    df_transacciones = df_bronze.filter(
        col("tipo_registro") == "TRANSACCION"
    )

    df_control = df_bronze.filter(
        col("tipo_registro") == "CONTROL"
    )

    # --------------------------------------------------------
    # 9.1 Conteo real de transacciones por archivo
    # --------------------------------------------------------

    conteos_reales = (
        df_transacciones
        .groupBy("source_file")
        .agg(
            count("*").alias("conteo_real")
        )
    )

    # --------------------------------------------------------
    # 9.2 Limpieza y clasificación de MONTO
    # --------------------------------------------------------
    # Se eliminan símbolos monetarios y separadores de miles.
    #
    # Ejemplo:
    # $1,200.50 -> 1200.50
    #
    # Se diferencian:
    # MONTO_NULO: valor vacío o NULL.
    # MONTO_FORMATO_INVALIDO: valor presente que no convierte.
    # MONTO_VALIDO: valor convertible a decimal.

    transacciones_validacion = (
        df_transacciones
        .withColumn(
            "monto_limpio",
            regexp_replace(
                trim(col("MONTO")),
                r"[$,]",
                ""
            )
        )
        .withColumn(
            "monto_decimal",
            when(
                col("monto_limpio").isNotNull()
                & (col("monto_limpio") != ""),
                col("monto_limpio").cast("decimal(18,2)")
            ).otherwise(lit(None).cast("decimal(18,2)"))
        )
        .withColumn(
            "estado_monto",
            when(
                col("monto_limpio").isNull()
                | (col("monto_limpio") == ""),
                "MONTO_NULO"
            )
            .when(
                col("monto_decimal").isNull(),
                "MONTO_FORMATO_INVALIDO"
            )
            .otherwise("MONTO_VALIDO")
        )
    )

    # --------------------------------------------------------
    # 9.3 Sumas y problemas de MONTO por archivo
    # --------------------------------------------------------

    sumas_reales = (
        transacciones_validacion
        .groupBy("source_file")
        .agg(
            spark_sum("monto_decimal").alias("suma_real"),

            spark_sum(
                when(
                    col("estado_monto") == "MONTO_NULO",
                    1
                ).otherwise(0)
            ).alias("montos_nulos"),

            spark_sum(
                when(
                    col("estado_monto") == "MONTO_FORMATO_INVALIDO",
                    1
                ).otherwise(0)
            ).alias("montos_invalidos")
        )
    )

    # --------------------------------------------------------
    # 9.4 Valores declarados en los registros TRL
    # --------------------------------------------------------

    control_archivos = (
        df_control
        .select(
            "source_file",

            col("trl_conteo")
            .cast("long")
            .alias("conteo_trl"),

            regexp_replace(
                trim(col("trl_suma_monto")),
                r"[$,]",
                ""
            )
            .cast("decimal(18,2)")
            .alias("suma_trl")
        )
    )

    # --------------------------------------------------------
    # 9.5 Consolidación de resultados por archivo
    # --------------------------------------------------------

    validacion = (
        conteos_reales
        .join(
            sumas_reales,
            on="source_file",
            how="full_outer"
        )
        .join(
            control_archivos,
            on="source_file",
            how="full_outer"
        )
    )

    # --------------------------------------------------------
    # 9.6 Comparación de conteo y suma
    # --------------------------------------------------------
    # La suma solo se considera válida cuando:
    # - El TRL tiene una suma convertible.
    # - La suma real se puede calcular.
    # - No existen montos nulos.
    # - No existen montos con formato inválido.
    #
    # No se reemplazan montos faltantes por cero.

    validacion = (
        validacion
        .withColumn(
            "conteo_valido",
            col("conteo_trl") == col("conteo_real")
        )
        .withColumn(
            "suma_valida",
            when(
                col("suma_trl").isNotNull()
                & col("suma_real").isNotNull()
                & (col("montos_nulos") == 0)
                & (col("montos_invalidos") == 0),

                spark_abs(
                    col("suma_trl") - col("suma_real")
                ) < 0.01
            ).otherwise(False)
        )
        .withColumn(
            "resultado",
            when(
                col("conteo_trl").isNull(),
                "SIN_TRL"
            )
            .when(
                col("conteo_real").isNull(),
                "TRL_SIN_TRANSACCIONES"
            )
            .when(
                (col("conteo_valido") == False)
                & (col("montos_nulos") > 0)
                & (col("montos_invalidos") > 0),
                "ERROR_CONTEO_Y_MONTO_NULO_E_INVALIDO"
            )
            .when(
                (col("conteo_valido") == False)
                & (col("montos_nulos") > 0),
                "ERROR_CONTEO_Y_MONTO_NULO"
            )
            .when(
                (col("conteo_valido") == False)
                & (col("montos_invalidos") > 0),
                "ERROR_CONTEO_Y_FORMATO_MONTO"
            )
            .when(
                col("conteo_valido") == False,
                "ERROR_CONTEO"
            )
            .when(
                (col("montos_nulos") > 0)
                & (col("montos_invalidos") > 0),
                "MONTO_NULO_E_INVALIDO"
            )
            .when(
                col("montos_nulos") > 0,
                "MONTO_NULO"
            )
            .when(
                col("montos_invalidos") > 0,
                "ERROR_FORMATO_MONTO"
            )
            .when(
                col("suma_valida"),
                "OK"
            )
            .otherwise("ERROR_MONTO")
        )
    )

    # --------------------------------------------------------
    # 9.7 Resultado detallado por archivo
    # --------------------------------------------------------

    print("Resultado de validación TRL por archivo:")

    validacion.select(
        "source_file",
        "conteo_trl",
        "conteo_real",
        "suma_trl",
        "suma_real",
        "montos_nulos",
        "montos_invalidos",
        "conteo_valido",
        "suma_valida",
        "resultado"
    ).orderBy(
        "source_file"
    ).show(100, truncate=False)

    # --------------------------------------------------------
    # 9.8 Resumen de calidad
    # --------------------------------------------------------

    print("Resumen de validación TRL:")

    validacion.groupBy(
        "resultado"
    ).count().show()

    print("Total de archivos con diferencias:")

    print(
        validacion.filter(
            col("resultado") != "OK"
        ).count()
    )

    # ========================================================
    # 9.9 DETALLE DE MONTOS NULOS E INVÁLIDOS
    # ========================================================

    print("=" * 60)
    print("DETALLE DE MONTOS NULOS E INVÁLIDOS")
    print("=" * 60)

    print("Distribución de estados de MONTO:")

    transacciones_validacion.groupBy(
        "estado_monto"
    ).count().show()

    print("Registros con MONTO nulo o formato inválido:")

    (
        transacciones_validacion
        .filter(
            col("estado_monto") != "MONTO_VALIDO"
        )
        .groupBy(
            "source_file",
            "MONTO",
            "estado_monto"
        )
        .count()
        .orderBy(
            "source_file"
        )
        .show(200, truncate=False)
    )

    # ========================================================
    # 10. VALIDACIÓN FINAL DE BRONZE
    # ========================================================

    print("=" * 60)
    print("RESUMEN FINAL BRONZE MÉXICO")
    print("=" * 60)

    print("Ola procesada:", OLA)

    print(
        "Total de registros Bronze:",
        df_bronze.count()
    )

    print(
        "Total de archivos Bronze:",
        df_bronze.select("source_file").distinct().count()
    )

    print(
        "Total de transacciones:",
        df_bronze.filter(
            col("tipo_registro") == "TRANSACCION"
        ).count()
    )

    print(
        "Total de registros TRL:",
        df_bronze.filter(
            col("tipo_registro") == "CONTROL"
        ).count()
    )

    print("=" * 60)
    print("INGESTA BRONZE MÉXICO FINALIZADA")
    print("=" * 60)

finally:
    spark.stop()
