
from pathlib import Path
import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. RUTAS DEL PROYECTO
# ============================================================

PROYECTO = Path(__file__).resolve().parents[2]

RUTA_SILVER = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "silver"
    / "transacciones_sql"
)

RUTA_CRM = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "silver"
    / "crm_comercios_vigente"
)

RUTA_SQL = PROYECTO / "sql" / "consultas"

RUTA_RESULTADOS = (
    PROYECTO
    / "resultados"
    / "consultas_sql"
)

RUTA_RESULTADOS.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. CONFIGURAR SPARK + DELTA
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
    .appName("Exportacion_Consultas_SQL_Pulso")
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
# 3. REGISTRAR TABLAS DELTA COMO VISTAS
# ============================================================

def registrar_delta(nombre_vista, ruta):

    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe la tabla Delta: {ruta}"
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
        f"Vista registrada: {nombre_vista} "
        f"| Registros: {df.count()}"
    )

    return df


# ============================================================
# 4. CONSULTAS SQL
# ============================================================

consultas = {
    "01_top_10_comercios_julio":
        "01_top_10_comercios_julio.sql",

    "02_tasa_reverso":
        "02_tasa_reverso.sql",

    "03_mayor_caida_transacciones":
        "03_mayor_caida_transacciones.sql",

    "04_comercios_sin_transacciones_julio":
        "04_comercios_sin_transacciones_julio.sql",
}


# ============================================================
# 5. EJECUCION Y EXPORTACION
# ============================================================

try:

    print("\n========== REGISTRO DE TABLAS SILVER ==========")

    # Registrar transacciones consolidadas
    registrar_delta(
        "silver_consolidada",
        RUTA_SILVER
    )

    # Registrar CRM vigente para las consultas comerciales
    registrar_delta(
        "silver_crm_comercios",
        RUTA_CRM
    )

    print("\n========== EJECUCION DE CONSULTAS SQL ==========")

    for nombre, archivo in consultas.items():

        ruta_archivo = RUTA_SQL / archivo

        if not ruta_archivo.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo SQL: {ruta_archivo}"
            )

        print(f"\nEjecutando consulta: {archivo}")

        contenido = ruta_archivo.read_text(
            encoding="utf-8"
        )

        df_resultado = spark.sql(contenido)

        total = df_resultado.count()

        ruta_csv = (
            RUTA_RESULTADOS
            / f"{nombre}.csv"
        )

        df_resultado.toPandas().to_csv(
            ruta_csv,
            index=False,
            encoding="utf-8-sig"
        )

        print(f"Registros obtenidos: {total}")
        print(f"Resultado generado: {ruta_csv}")

    print("\nTODAS LAS CONSULTAS FUERON EJECUTADAS CORRECTAMENTE.")

finally:

    spark.stop()
    print("\nSesion Spark finalizada.")


