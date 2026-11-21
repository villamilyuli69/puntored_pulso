
from pathlib import Path
import argparse
import hashlib
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    input_file_name,
    lit,
    regexp_extract,
    create_map,
    element_at,
    struct
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# ==================================================
# 1. CONFIGURACIÓN GENERAL
# ==================================================

PROYECTO = Path(__file__).resolve().parents[2]

RUTA_DATASET = (
    PROYECTO / "dataset_pulso_ESENCIAL-2026"
)

RUTA_BRONZE = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "bronze"
    / "co_transacciones_cdc"
)

OLAS_DISPONIBLES = ["ola_1", "ola_2"]


# ==================================================
# 2. CONFIGURACIÓN DE PYTHON PARA SPARK
# ==================================================

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE

print("\nPython utilizado por Spark:")
print(PYTHON_EXECUTABLE)


# ==================================================
# 3. ARGUMENTOS DE EJECUCIÓN
# ==================================================

parser = argparse.ArgumentParser(
    description="Ingesta CDC Colombia hacia Bronze"
)

parser.add_argument(
    "--ola",
    choices=["ola_1", "ola_2", "ambas"],
    default="ambas",
    help="Ola que se desea procesar"
)

args = parser.parse_args()

olas = (
    OLAS_DISPONIBLES
    if args.ola == "ambas"
    else [args.ola]
)


# ==================================================
# 4. FUNCIONES AUXILIARES
# ==================================================

def obtener_archivos(ola):
    """
    Obtiene todos los archivos JSONL de una ola.
    """

    ruta_origen = (
        RUTA_DATASET
        / ola
        / "landing"
        / "co"
        / "core_transacciones_cdc"
    )

    if not ruta_origen.exists():
        raise FileNotFoundError(
            f"No existe la ruta de origen: {ruta_origen}"
        )

    archivos = sorted(
        ruta_origen.rglob("*.jsonl")
    )

    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron archivos JSONL para {ola}"
        )

    return archivos


def calcular_checksum(ruta_archivo):
    """
    Calcula SHA-256 del contenido original del archivo.
    """

    sha256 = hashlib.sha256()

    with open(ruta_archivo, "rb") as archivo:

        for bloque in iter(
            lambda: archivo.read(1024 * 1024),
            b""
        ):
            sha256.update(bloque)

    return sha256.hexdigest()


def generar_id_ingesta(ruta_relativa, checksum):
    """
    Identificador determinístico por ruta y contenido.
    """

    identificador = (
        f"{ruta_relativa}|{checksum}"
    )

    return hashlib.sha256(
        identificador.encode("utf-8")
    ).hexdigest()


# ==================================================
# 5. CREACIÓN DE SPARK SESSION
# ==================================================

builder = (
    SparkSession.builder
    .appName("Ingesta_Colombia_Bronze")
    .master("local[*]")
    .config(
        "spark.sql.session.timeZone",
        "America/Bogota"
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.pyspark.python",
        PYTHON_EXECUTABLE
    )
    .config(
        "spark.pyspark.driver.python",
        PYTHON_EXECUTABLE
    )
    .config(
        "spark.executorEnv.PYSPARK_PYTHON",
        PYTHON_EXECUTABLE
    )
    .config(
        "spark.python.worker.reuse",
        "true"
    )
)

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()


try:

    spark.sparkContext.setLogLevel("WARN")

    # ==================================================
    # 6. IDENTIFICAR ARCHIVOS DE LAS OLAS
    # ==================================================

    print("\nIdentificando archivos de origen...")

    metadatos_archivos = []

    for ola in olas:

        archivos = obtener_archivos(ola)

        print(
            f"{ola}: {len(archivos)} archivos encontrados"
        )

        for archivo in archivos:

            ruta_relativa = str(
                archivo.relative_to(PROYECTO)
            ).replace("\\", "/")

            checksum = calcular_checksum(
                archivo
            )

            ingestion_file_id = generar_id_ingesta(
                ruta_relativa,
                checksum
            )

            metadatos_archivos.append({
                "nombre_archivo": archivo.name,
                "ruta": str(archivo.resolve()),
                "source_file": ruta_relativa,
                "checksum": checksum,
                "ingestion_file_id": ingestion_file_id,
                "ola": ola
            })

    # El nombre del archivo se usa para asociar
    # los metadatos con input_file_name().
    # Validamos que no haya nombres repetidos.

    nombres_archivos = [
        registro["nombre_archivo"]
        for registro in metadatos_archivos
    ]

    if len(nombres_archivos) != len(set(nombres_archivos)):
        raise ValueError(
            "Existen nombres de archivo repetidos entre las olas. "
            "Se requiere una asociación por ruta completa."
        )

    # ==================================================
    # 7. IDENTIFICAR ARCHIVOS YA PROCESADOS
    # ==================================================

    RUTA_BRONZE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    ids_existentes = set()

    tabla_existe = DeltaTable.isDeltaTable(
        spark,
        str(RUTA_BRONZE)
    )

    if tabla_existe:

        print("\nConsultando archivos previamente ingeridos...")

        df_existente = (
            spark.read
            .format("delta")
            .load(str(RUTA_BRONZE))
        )

        columnas_requeridas = {
            "ingestion_file_id",
            "source_file",
            "raw_record"
        }

        columnas_actuales = set(df_existente.columns)

        faltantes = (
            columnas_requeridas - columnas_actuales
        )

        if faltantes:
            raise ValueError(
                "La tabla Bronze existente tiene un esquema "
                f"incompatible. Faltan columnas: {faltantes}. "
                "Respalda la tabla y recréala antes de continuar."
            )

        ids_existentes = {
            fila["ingestion_file_id"]
            for fila in (
                df_existente
                .select("ingestion_file_id")
                .distinct()
                .collect()
            )
        }

    archivos_nuevos = [
        registro
        for registro in metadatos_archivos
        if registro["ingestion_file_id"] not in ids_existentes
    ]

    print("\nRESUMEN DE ARCHIVOS")
    print(
        f"Archivos identificados: {len(metadatos_archivos)}"
    )
    print(
        f"Archivos previamente procesados: {len(ids_existentes)}"
    )
    print(
        f"Archivos nuevos: {len(archivos_nuevos)}"
    )

    # ==================================================
    # 8. PROCESAR ARCHIVOS NUEVOS
    # ==================================================

    if not archivos_nuevos:

        print(
            "\nNo existen archivos nuevos para ingerir."
        )

    else:

        rutas_nuevas = [
            registro["ruta"]
            for registro in archivos_nuevos
        ]

        # ----------------------------------------------
        # 8.1 Inferir esquema de los eventos CDC
        # ----------------------------------------------

        print("\nInfiriendo esquema CDC...")

        schema_origen = (
            spark.read
            .option("multiLine", "false")
            .json(rutas_nuevas)
            .schema
        )

        # ----------------------------------------------
        # 8.2 Leer todos los JSONL en conjunto
        # ----------------------------------------------

        print(
            "\nLeyendo todos los archivos nuevos..."
        )

        df_texto = (
            spark.read
            .text(rutas_nuevas)
            .withColumnRenamed(
                "value",
                "raw_record"
            )
            .withColumn(
                "_nombre_archivo",
                regexp_extract(
                    input_file_name(),
                    r"([^/]+)$",
                    1
                )
            )
        )

        # ----------------------------------------------
        # 8.3 Construir mapa de metadatos
        # ----------------------------------------------

        argumentos_mapa = []

        for registro in archivos_nuevos:

            metadato = struct(
                lit(registro["source_file"]).alias(
                    "source_file"
                ),
                lit(registro["checksum"]).alias(
                    "source_file_checksum"
                ),
                lit(registro["ingestion_file_id"]).alias(
                    "ingestion_file_id"
                ),
                lit(registro["ola"]).alias(
                    "ingestion_wave"
                )
            )

            argumentos_mapa.extend([
                lit(registro["nombre_archivo"]),
                metadato
            ])

        mapa_metadatos = create_map(
            *argumentos_mapa
        )

        # ----------------------------------------------
        # 8.4 Interpretar JSON y agregar metadatos
        # ----------------------------------------------

        df_bronze = (
            df_texto
            .withColumn(
                "_evento",
                from_json(
                    col("raw_record"),
                    schema_origen
                )
            )
            .withColumn(
                "_metadato",
                element_at(
                    mapa_metadatos,
                    col("_nombre_archivo")
                )
            )
            .select(
                "_evento.*",
                "raw_record",
                col("_metadato.source_file").alias(
                    "source_file"
                ),
                col(
                    "_metadato.source_file_checksum"
                ).alias(
                    "source_file_checksum"
                ),
                col(
                    "_metadato.ingestion_file_id"
                ).alias(
                    "ingestion_file_id"
                ),
                col(
                    "_metadato.ingestion_wave"
                ).alias(
                    "ingestion_wave"
                )
            )
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
        )

        # ----------------------------------------------
        # 8.5 Validar asociación de metadatos
        # ----------------------------------------------

        archivos_sin_metadatos = (
            df_bronze
            .filter(
                col("ingestion_file_id").isNull()
            )
            .limit(1)
            .count()
        )

        if archivos_sin_metadatos > 0:

            raise ValueError(
                "Existen registros sin metadatos de archivo. "
                "No se realizará la escritura en Bronze."
            )

        # ----------------------------------------------
        # 8.6 Validar JSON no interpretado
        # ----------------------------------------------

        registros_invalidos = (
            df_bronze
            .filter(
                col("raw_record").isNotNull()
            )
            .filter(
                col("topic").isNull()
                & col("key").isNull()
                & col("value").isNull()
            )
            .count()
        )

        if registros_invalidos > 0:

            raise ValueError(
                f"Se encontraron {registros_invalidos} "
                "registros que no pudieron interpretarse. "
                "No se escribirá Bronze."
            )

        # ----------------------------------------------
        # 8.7 Escritura Delta agrupada
        # ----------------------------------------------

        print("\nEscribiendo registros en Bronze...")

        (
            df_bronze
            .write
            .format("delta")
            .mode("append")
            .save(str(RUTA_BRONZE))
        )

        print(
            "\nEscritura Delta finalizada correctamente."
        )

    # ==================================================
    # 9. VALIDACIONES FINALES
    # ==================================================

    print("\nConsultando tabla Bronze...")

    df_final = (
        spark.read
        .format("delta")
        .load(str(RUTA_BRONZE))
    )

    total_bronze = df_final.count()

    print("\n" + "=" * 60)
    print("RESULTADO BRONZE - COLOMBIA")
    print("=" * 60)

    print(
        f"Total acumulado Bronze: {total_bronze}"
    )

    print("\nREGISTROS POR OLA:")

    (
        df_final
        .groupBy("ingestion_wave")
        .count()
        .orderBy("ingestion_wave")
        .show()
    )

    print("\nEVENTOS POR OPERACIÓN CDC:")

    (
        df_final
        .groupBy(
            col("value.op").alias("operacion_cdc")
        )
        .count()
        .orderBy("operacion_cdc")
        .show()
    )

    print("\nARCHIVOS INGRESADOS POR OLA:")

    (
        df_final
        .select(
            "ingestion_file_id",
            "ingestion_wave"
        )
        .distinct()
        .groupBy("ingestion_wave")
        .count()
        .show()
    )

    print("\nESQUEMA FINAL BRONZE:")

    df_final.printSchema()

    print("\nRUTA BRONZE:")
    print(RUTA_BRONZE)

    print("\nINGESTA BRONZE COLOMBIA FINALIZADA.")

finally:

    spark.stop()
