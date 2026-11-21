
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    input_file_name,
    explode
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================

OLA = "ola_1"
CATALOG_VERSION = "2026-06-01"

RUTA_LANDING = os.path.join(
    "dataset_pulso_ESENCIAL-2026",
    OLA,
    "landing",
    "catalogos"
)

RUTA_BRONZE = os.path.join(
    "data",
    "lakehouse",
    "bronze"
)

RUTA_PRODUCTOS = os.path.join(
    RUTA_LANDING,
    "productos",
    f"version={CATALOG_VERSION}",
    "productos.json"
)

RUTA_CANALES = os.path.join(
    RUTA_LANDING,
    "canales",
    "canales.csv"
)

RUTA_TAXONOMIA = os.path.join(
    RUTA_LANDING,
    "taxonomia",
    "lineas_negocio_corporativas.csv"
)


# ============================================================
# 2. VALIDAR EXISTENCIA DE LOS ARCHIVOS
# ============================================================

for ruta in [RUTA_PRODUCTOS, RUTA_CANALES, RUTA_TAXONOMIA]:
    if not os.path.isfile(ruta):
        raise FileNotFoundError(
            f"No se encontró el archivo de origen: {ruta}"
        )


# ============================================================
# 3. CREAR SESIÓN SPARK + DELTA
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
    .appName("Bronze_Catalogos")
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
# 4. FUNCIÓN DE ESCRITURA DELTA
# ============================================================

def escribir_delta(df, ruta_destino, condicion_reemplazo):

    if DeltaTable.isDeltaTable(spark, ruta_destino):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", condicion_reemplazo)
            .save(ruta_destino)
        )

        print(f"Registros reemplazados según condición: {condicion_reemplazo}")

    else:

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(ruta_destino)
        )

    print(f"Tabla Delta almacenada en: {ruta_destino}")


# ============================================================
# 5. INGESTA DE PRODUCTOS JSON
# ============================================================

print("\n========== INGESTA PRODUCTOS ==========")

df_productos_documento = (
    spark.read
    .option("multiLine", "true")
    .json(RUTA_PRODUCTOS)
)

# Se conserva la información general del documento JSON
# y se agrega la metadata de ingesta.

df_productos_documento = (
    df_productos_documento
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_wave", lit(OLA))
)

# El JSON contiene un arreglo de productos.
# Se genera un registro por producto, manteniendo
# las estructuras anidadas comision y limites.

df_productos = (
    df_productos_documento
    .select(
        col("version").alias("catalog_version"),
        col("generado_por"),
        col("source_file"),
        col("ingestion_timestamp"),
        col("ingestion_wave"),
        explode(col("productos")).alias("producto")
    )
    .select(
        "catalog_version",
        "generado_por",
        "source_file",
        "ingestion_timestamp",
        "ingestion_wave",
        "producto.*"
    )
)

print("Total productos leídos:", df_productos.count())

ruta_bronze_productos = os.path.join(
    RUTA_BRONZE,
    "catalogos_productos"
)

escribir_delta(
    df_productos,
    ruta_bronze_productos,
    f"ingestion_wave = '{OLA}' AND catalog_version = '{CATALOG_VERSION}'"
)


# ============================================================
# 6. INGESTA DE CANALES CSV
# ============================================================

print("\n========== INGESTA CANALES ==========")

df_canales = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(RUTA_CANALES)
)

df_canales = (
    df_canales
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_wave", lit(OLA))
)

print("Total canales leídos:", df_canales.count())

ruta_bronze_canales = os.path.join(
    RUTA_BRONZE,
    "catalogos_canales"
)

escribir_delta(
    df_canales,
    ruta_bronze_canales,
    f"ingestion_wave = '{OLA}'"
)


# ============================================================
# 7. INGESTA DE TAXONOMÍA CSV
# ============================================================

print("\n========== INGESTA TAXONOMÍA ==========")

df_taxonomia = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(RUTA_TAXONOMIA)
)

df_taxonomia = (
    df_taxonomia
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_wave", lit(OLA))
)

print("Total registros de taxonomía leídos:", df_taxonomia.count())

ruta_bronze_taxonomia = os.path.join(
    RUTA_BRONZE,
    "catalogos_taxonomia"
)

escribir_delta(
    df_taxonomia,
    ruta_bronze_taxonomia,
    f"ingestion_wave = '{OLA}'"
)


# ============================================================
# 8. VALIDACIONES DE LAS TABLAS BRONZE
# ============================================================

print("\n========== VALIDACIONES BRONZE ==========")

tablas = {
    "PRODUCTOS": ruta_bronze_productos,
    "CANALES": ruta_bronze_canales,
    "TAXONOMIA": ruta_bronze_taxonomia
}

for nombre, ruta in tablas.items():

    df = spark.read.format("delta").load(ruta)

    print(f"\n--- {nombre} ---")
    print("Total registros:", df.count())

    df.printSchema()
    df.show(5, truncate=False)


# Validación de duplicados en productos.
# La llave de negocio se compone de país y código de producto.

df_productos_delta = spark.read.format("delta").load(
    ruta_bronze_productos
)

duplicados_productos = (
    df_productos_delta
    .groupBy("pais", "producto_cod")
    .count()
    .filter(col("count") > 1)
)

print(
    "Llaves duplicadas en productos:",
    duplicados_productos.count()
)

# Validación de duplicados en canales.

df_canales_delta = spark.read.format("delta").load(
    ruta_bronze_canales
)

duplicados_canales = (
    df_canales_delta
    .groupBy("pais", "codigo_origen")
    .count()
    .filter(col("count") > 1)
)

print(
    "Llaves duplicadas en canales:",
    duplicados_canales.count()
)

# Validación de duplicados en taxonomía.

df_taxonomia_delta = spark.read.format("delta").load(
    ruta_bronze_taxonomia
)

duplicados_taxonomia = (
    df_taxonomia_delta
    .groupBy("linea_negocio_corporativa")
    .count()
    .filter(col("count") > 1)
)

print(
    "Llaves duplicadas en taxonomía:",
    duplicados_taxonomia.count()
)

print("\n========== INGESTA CATÁLOGOS FINALIZADA ==========")

spark.stop()
