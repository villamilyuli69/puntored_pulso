
import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    when,
    regexp_replace,
    count,
    sum as spark_sum,
    abs as spark_abs,
    round as spark_round,
    first
)
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CONFIGURACION
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROYECTO = Path(__file__).resolve().parents[2]

RUTA_BRONZE = (
    PROYECTO / "data" / "lakehouse" / "bronze" / "mx_transacciones"
)

RUTA_CUARENTENA = (
    PROYECTO / "data" / "lakehouse" / "bronze" / "quarantine" / "mx_transacciones"
)

RUTA_HALLAZGOS = (
    PROYECTO / "data" / "lakehouse" / "bronze" / "quality" / "hallazgos_calidad"
)

RUTA_VALIDACION_TRL = (
    PROYECTO / "data" / "lakehouse" / "bronze" / "quality" / "validacion_trl_mexico"
)


# ============================================================
# 2. SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("Calidad_Mexico")
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


# ============================================================
# 3. LECTURA DE BRONZE
# ============================================================

print("Leyendo Bronze México...")

df_bronze = (
    spark.read
    .format("delta")
    .load(str(RUTA_BRONZE))
)

df_transacciones = df_bronze.filter(
    col("tipo_registro") == "TRANSACCION"
)

df_control = df_bronze.filter(
    col("tipo_registro") == "CONTROL"
)


# ============================================================
# 4. REGLAS DE CALIDAD
# ============================================================

# Limpiar formato monetario para convertir a decimal.
# El valor original de Bronze no se modifica.

df_evaluado = (
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
        col("monto_limpio").cast("decimal(18,2)")
    )
    .withColumn(
        "es_monto_nulo",
        col("MONTO").isNull() |
        (trim(col("MONTO")) == "")
    )
    .withColumn(
        "es_monto_invalido",
        (~col("es_monto_nulo")) &
        col("monto_decimal").isNull()
    )
)


# ============================================================
# 5. TABLA DE CUARENTENA
# ============================================================

df_cuarentena = (
    df_evaluado
    .filter(
        col("es_monto_nulo") |
        col("es_monto_invalido")
    )
    .withColumn(
        "campo_afectado",
        lit("MONTO")
    )
    .withColumn(
        "valor_original",
        col("MONTO")
    )
    .withColumn(
        "motivo_cuarentena",
        when(
            col("es_monto_nulo"),
            "MONTO_NULO"
        ).otherwise("MONTO_INVALIDO")
    )
    .withColumn(
        "severidad",
        lit("CUARENTENA")
    )
)

(
    df_cuarentena.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(RUTA_CUARENTENA))
)

print("Registros en cuarentena:", df_cuarentena.count())


# ============================================================
# 6. CONTEO Y SUMA REAL POR ARCHIVO
# ============================================================

resumen_transacciones = (
    df_evaluado
    .groupBy("source_file")
    .agg(
        count("*").alias("conteo_real"),

        spark_sum("monto_decimal").alias("suma_real"),

        spark_sum(
            when(col("es_monto_nulo"), 1).otherwise(0)
        ).alias("montos_nulos"),

        spark_sum(
            when(col("es_monto_invalido"), 1).otherwise(0)
        ).alias("montos_invalidos")
    )
)


# ============================================================
# 7. OBTENER VALORES DECLARADOS EN TRL
# ============================================================

resumen_control = (
    df_control
    .groupBy("source_file")
    .agg(
        count("*").alias("cantidad_trl"),

        first(
            col("trl_conteo").cast("long"),
            ignorenulls=True
        ).alias("conteo_trl"),

        first(
            col("trl_suma_monto").cast("decimal(18,2)"),
            ignorenulls=True
        ).alias("suma_trl")
    )
)


# ============================================================
# 8. VALIDACION TRL
# ============================================================

validacion_trl = (
    resumen_transacciones
    .join(
        resumen_control,
        "source_file",
        "full"
    )
    .withColumn(
        "conteo_valido",
        (col("cantidad_trl") == 1) &
        (col("conteo_trl") == col("conteo_real"))
    )
    .withColumn(
        "suma_verificable",
        (col("montos_nulos") == 0) &
        (col("montos_invalidos") == 0) &
        col("suma_trl").isNotNull() &
        col("suma_real").isNotNull()
    )
    .withColumn(
        "suma_valida",
        when(
            col("suma_verificable"),
            spark_abs(
                col("suma_trl") - col("suma_real")
            ) < 0.01
        ).otherwise(lit(None).cast("boolean"))
    )

    .withColumn(
        "resultado",
        when(
            col("cantidad_trl").isNull(),
            "TRL_AUSENTE"
        )
        .when(
            col("cantidad_trl") != 1,
            "TRL_DUPLICADO"
        )
        .when(
            (~col("conteo_valido")) &
            col("suma_verificable") &
            (~col("suma_valida")),
            "DIFERENCIA_CONTEO_Y_MONTO"
        )
        .when(
            ~col("conteo_valido"),
            "DIFERENCIA_CONTEO"
        )
        .when(
            col("montos_nulos") > 0,
            "MONTO_NO_VERIFICABLE"
        )
        .when(
            col("montos_invalidos") > 0,
            "MONTO_INVALIDO"
        )
        .when(
            col("suma_trl").isNull(),
            "TRL_MONTO_INVALIDO"
        )
        .when(
            col("suma_verificable") & (~col("suma_valida")),
            "DIFERENCIA_MONTO"
        )
        .otherwise("OK")
    )

       .withColumn(
        "estado_publicacion",
        when(
            col("resultado").isin(
                "TRL_AUSENTE",
                "TRL_DUPLICADO",
                "DIFERENCIA_CONTEO",
                "DIFERENCIA_MONTO",
                "DIFERENCIA_CONTEO_Y_MONTO",
                "TRL_MONTO_INVALIDO"
            ),
            "BLOQUEADO"
        )
        .when(
            col("resultado").isin(
                "MONTO_NO_VERIFICABLE",
                "MONTO_INVALIDO"
            ),
            "APTO_PARCIAL"
        )
        .otherwise("APTO")
    )
    .withColumn(
        "pais",
        lit("MX")
    )
)

(
    validacion_trl.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(RUTA_VALIDACION_TRL))
)


# ============================================================
# 9. CONSTRUIR TABLA DE HALLAZGOS
# ============================================================

total_por_archivo = col("conteo_real")

hallazgos_nulos = (
    validacion_trl
    .filter(col("montos_nulos") > 0)
    .select(
        lit("MX_TRANSACCIONES").alias("fuente"),
        lit("MONTO_NULO").alias("hallazgo"),
        col("source_file").alias("archivo_origen"),
        col("montos_nulos").cast("long").alias("cantidad_afectada"),
        total_por_archivo.cast("long").alias("total_evaluado"),
        spark_round(
            col("montos_nulos") * 100.0 /
            col("conteo_real"),
            2
        ).alias("porcentaje_afectado"),
        lit("CUARENTENA").alias("severidad"),
        lit("Los registros no pueden aportar a las cifras oficiales.")
        .alias("impacto"),
        lit("Separar únicamente las transacciones afectadas.")
        .alias("decision")
    )
)

hallazgos_invalidos = (
    validacion_trl
    .filter(col("montos_invalidos") > 0)
    .select(
        lit("MX_TRANSACCIONES").alias("fuente"),
        lit("MONTO_INVALIDO").alias("hallazgo"),
        col("source_file").alias("archivo_origen"),
        col("montos_invalidos").cast("long").alias("cantidad_afectada"),
        total_por_archivo.cast("long").alias("total_evaluado"),
        spark_round(
            col("montos_invalidos") * 100.0 /
            col("conteo_real"),
            2
        ).alias("porcentaje_afectado"),
        lit("CUARENTENA").alias("severidad"),
        lit("Monto no convertible a decimal.")
        .alias("impacto"),
        lit("Separar los registros afectados.")
        .alias("decision")
    )
)

hallazgos_trl = (
    validacion_trl
    .filter(
        col("resultado").isin(
            "TRL_AUSENTE",
            "TRL_DUPLICADO",
            "DIFERENCIA_CONTEO",
            "DIFERENCIA_MONTO",
            "DIFERENCIA_CONTEO_Y_MONTO",
            "TRL_MONTO_INVALIDO"
        )
    )
    .select(
        lit("MX_TRANSACCIONES").alias("fuente"),
        col("resultado").alias("hallazgo"),
        col("source_file").alias("archivo_origen"),
        lit(1).cast("long").alias("cantidad_afectada"),
        lit(1).cast("long").alias("total_evaluado"),
        lit(100.0).alias("porcentaje_afectado"),
        lit("BLOQUEA").alias("severidad"),
        lit("El control TRL no coincide con los datos cargados.")
        .alias("impacto"),
        lit("Bloquear la publicación del archivo hasta investigar.")
        .alias("decision")
    )
)

hallazgos = (
    hallazgos_nulos
    .unionByName(hallazgos_invalidos)
    .unionByName(hallazgos_trl)
)

(
    hallazgos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(RUTA_HALLAZGOS))
)


# ============================================================
# 10. RESULTADOS
# ============================================================

print("====================================")
print("RESUMEN DE CUARENTENA")
print("====================================")

df_cuarentena.groupBy(
    "motivo_cuarentena"
).count().show()

print("====================================")
print("VALIDACION TRL POR ARCHIVO")
print("====================================")

validacion_trl.select(
    "source_file",
    "conteo_trl",
    "conteo_real",
    "suma_trl",
    "suma_real",
    "montos_nulos",
    "montos_invalidos",
    "resultado",
    "estado_publicacion"
).orderBy("source_file").show(100, truncate=False)

print("====================================")
print("RESUMEN DE HALLAZGOS")
print("====================================")

hallazgos.groupBy(
    "hallazgo",
    "severidad"
).agg(
    spark_sum("cantidad_afectada").alias("cantidad")
).show(truncate=False)

print("====================================")
print("PROCESO DE CALIDAD FINALIZADO")
print("====================================")

spark.stop()
