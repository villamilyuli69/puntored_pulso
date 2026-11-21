import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from delta import configure_spark_with_delta_pip

# Configuración
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

RUTA_SILVER = "data/lakehouse/silver/transacciones"

builder = (
    SparkSession.builder
    .appName("Validar_Silver_Transacciones")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "America/Bogota")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    # Leer la tabla Delta
    df = spark.read.format("delta").load(RUTA_SILVER)

    print("\n========== VALIDACION SILVER ==========")

    # 1. Total de registros
    print("\n1. TOTAL DE REGISTROS")
    print("Total:", df.count())

    # 2. Registros por país
    print("\n2. REGISTROS POR PAIS")
    df.groupBy("pais").count().show()

    # 3. Estados
    print("\n3. ESTADOS")
    df.groupBy("pais", "estado").count().orderBy(
        "pais", "estado"
    ).show(30, False)

    # 4. Validar duplicados
    print("\n4. DUPLICADOS DE LLAVE")

    duplicados = (
        df.groupBy("pais", "id_transaccion")
        .count()
        .filter(col("count") > 1)
    )

    print("Cantidad de llaves duplicadas:", duplicados.count())
    duplicados.show(10, False)

    # 5. Validar campos obligatorios
    print("\n5. CAMPOS OBLIGATORIOS")

    for campo in [
        "pais",
        "id_transaccion",
        "comercio_id",
        "fecha_negocio",
        "monto",
        "moneda",
        "estado"
    ]:
        cantidad = df.filter(col(campo).isNull()).count()
        print(f"{campo}: {cantidad} nulos")

    # 6. Validar tipos de datos
    print("\n6. ESQUEMA DE LA TABLA")
    df.printSchema()

    # 7. Estados no homologados
    print("\n7. ESTADOS FUERA DEL DOMINIO")

    df.filter(
        ~col("estado").isin(
            "APROBADA",
            "RECHAZADA",
            "PENDIENTE",
            "REVERSADA"
        )
    ).groupBy("estado").count().show()

    # 8. Canales sin homologar
    print("\n8. CANALES SIN HOMOLOGAR")

    df.filter(
        col("canal_homologado").isNull()
    ).groupBy(
        "pais", "canal_codigo_origen"
    ).count().show(30, False)

    # 9. Líneas corporativas sin homologar
    print("\n9. LINEAS CORPORATIVAS SIN HOMOLOGAR")

    df.filter(
        col("linea_negocio_corporativa").isNull()
    ).groupBy(
        "pais", "linea_negocio_producto"
    ).count().show(30, False)

    # 10. Observaciones de calidad
    print("\n10. OBSERVACIONES DE CALIDAD")

    df.groupBy("observacion_calidad").count().orderBy(
        col("count").desc()
    ).show(30, False)

    # 11. Muestra de registros
    print("\n11. MUESTRA DE TRANSACCIONES")

    df.select(
        "pais",
        "id_transaccion",
        "comercio_id",
        "fecha_hora_negocio",
        "monto",
        "comision",
        "moneda",
        "estado",
        "canal_homologado",
        "linea_negocio_corporativa"
    ).show(20, False)

    print("\nVALIDACION FINALIZADA")

finally:
    spark.stop()
