
import os
import sys
import glob

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    input_file_name,
    regexp_extract
)
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

RUTA_BASE = r"dataset_pulso_ESENCIAL-2026"

OLAS = ["ola_1", "ola_2"]

RUTA_BRONZE = (
    r"data\lakehouse\bronze\crm_comercios"
)


# ============================================================
# 2. CREACIÓN DE SPARK SESSION CON DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName("Bronze_CRM")
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
# 3. LECTURA DE ARCHIVOS CSV
# ============================================================

print("Iniciando lectura de archivos CRM...")

dataframes = []

for ola in OLAS:

    ruta_origen = os.path.join(
        RUTA_BASE,
        ola,
        "landing",
        "crm",
        "comercios"
    )

    archivos = sorted(
        glob.glob(
            os.path.join(
                ruta_origen,
                "snapshot_fecha=*",
                "comercios.csv"
            )
        )
    )

    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron archivos CRM para {ola}: "
            f"{ruta_origen}"
        )

    print(f"{ola}: {len(archivos)} snapshots encontrados.")

    for archivo in archivos:

        print("Leyendo:", archivo)

        df_archivo = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("sep", ",")
            .option("inferSchema", "false")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .load(archivo)
        )

        df_archivo = (
            df_archivo
            .withColumn("source_file", input_file_name())
            .withColumn(
                "snapshot_date",
                regexp_extract(
                    col("source_file"),
                    r"snapshot_fecha=(\d{4}-\d{2}-\d{2})",
                    1
                )
            )
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_wave", lit(ola))
        )

        dataframes.append(df_archivo)


if not dataframes:
    raise ValueError("No se encontraron archivos para procesar.")


# Unificar los snapshots de las dos olas
df = dataframes[0]

for dataframe in dataframes[1:]:
    df = df.unionByName(dataframe)


print("Lectura de archivos CRM completada.")


# ============================================================
# 4. CONSERVAR CAMPOS ORIGINALES
# ============================================================

# Los campos originales se conservan como texto.
# No se normalizan fechas ni se eliminan duplicados en Bronze.

print("Estructura de los datos originales:")
df.printSchema()


# ============================================================
# 5. METADATOS DE INGESTA
# ============================================================

# Los metadatos source_file, snapshot_date,
# ingestion_timestamp e ingestion_wave
# permiten identificar el origen de cada registro.


# ============================================================
# 6. VALIDACIONES PREVIAS
# ============================================================

print("Total de registros leídos:", df.count())

print("Distribución por ola:")
df.groupBy("ingestion_wave").count().show()

print("Distribución por snapshot:")
df.groupBy(
    "ingestion_wave",
    "snapshot_date"
).count().orderBy(
    "ingestion_wave",
    "snapshot_date"
).show(20, truncate=False)

print("Muestra de registros CRM:")
df.show(10, truncate=False)


# ============================================================
# 7. ESCRITURA EN BRONZE DELTA
# ============================================================

print("Iniciando escritura incremental Delta...")

# Identificar exclusivamente los snapshots que llegan
# en esta ejecución.
snapshots_procesados = (
    df.select(
        "ingestion_wave",
        "snapshot_date"
    )
    .distinct()
    .collect()
)

if not snapshots_procesados:
    raise ValueError("No hay snapshots para escribir.")

# Construir una condición para reemplazar solamente
# los snapshots correspondientes a esta ejecución.
condiciones = []

for fila in snapshots_procesados:

    ola = fila["ingestion_wave"]
    fecha = fila["snapshot_date"]

    if not fecha:
        raise ValueError(
            "Se encontró un snapshot sin fecha."
        )

    condiciones.append(
        f"(ingestion_wave = '{ola}' "
        f"AND snapshot_date = '{fecha}')"
    )

condicion_reemplazo = " OR ".join(condiciones)

print("Snapshots que se van a actualizar:")

for fila in snapshots_procesados:
    print(
        f"Ola: {fila['ingestion_wave']} "
        f"| Snapshot: {fila['snapshot_date']}"
    )

# Si la tabla ya existe, reemplazar únicamente
# los snapshots recibidos en esta ejecución.
if DeltaTable.isDeltaTable(spark, RUTA_BRONZE):

    print("La tabla Bronze ya existe.")
    print("Actualizando únicamente los snapshots procesados...")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", condicion_reemplazo)
        .save(RUTA_BRONZE)
    )

else:

    print("La tabla Bronze no existe. Creando tabla inicial...")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(RUTA_BRONZE)
    )

print("Escritura incremental Delta exitosa.")

# ============================================================
# 8. VALIDACIÓN DE LA TABLA ESCRITA
# ============================================================

print("Validando información almacenada en Delta...")

df_bronze = (
    spark.read
    .format("delta")
    .load(RUTA_BRONZE)
)

print("Total de registros almacenados:", df_bronze.count())

print("Estructura de la tabla:")
df_bronze.printSchema()

print("Distribución final por ola:")
df_bronze.groupBy("ingestion_wave").count().show()

print("Distribución final por snapshot:")
df_bronze.groupBy(
    "ingestion_wave",
    "snapshot_date"
).count().orderBy(
    "ingestion_wave",
    "snapshot_date"
).show(20, truncate=False)

print("Muestra de datos almacenados:")
df_bronze.show(10, truncate=False)



# ============================================================
# 9. VALIDACIÓN FINAL
# ============================================================

print("Ejecutando validaciones finales...")

conteos_esperados = {
    "2026-06-01": 1931,
    "2026-06-08": 1926,
    "2026-06-15": 1928,
    "2026-06-22": 1938,
    "2026-06-29": 1942,
    "2026-07-06": 1963,
    "2026-07-13": 1975,
    "2026-07-20": 1972,
    "2026-07-27": 1983
}

# Identificar las fechas procesadas en esta ejecución.
fechas_procesadas = [
    fila["snapshot_date"]
    for fila in snapshots_procesados
]

# Obtener los conteos esperados únicamente
# para los snapshots procesados.
df_esperados = spark.createDataFrame(
    [
        (fecha, cantidad)
        for fecha, cantidad in conteos_esperados.items()
        if fecha in fechas_procesadas
    ],
    ["snapshot_date", "conteo_esperado"]
)

# Conteo real de los snapshots almacenados en Bronze.
conteos_reales = (
    df_bronze
    .filter(
        col("snapshot_date").isin(fechas_procesadas)
    )
    .groupBy("snapshot_date")
    .count()
    .withColumnRenamed("count", "conteo_real")
)

# Comparar conteos esperados contra los almacenados.
validacion = (
    df_esperados
    .join(
        conteos_reales,
        "snapshot_date",
        "left"
    )
    .withColumn(
        "conteo_valido",
        col("conteo_esperado") == col("conteo_real")
    )
)

print("Resultado de validación por snapshot:")

validacion.orderBy(
    "snapshot_date"
).show(20, truncate=False)

print("Resumen de validación:")

validacion.groupBy(
    "conteo_valido"
).count().show()

diferencias = validacion.filter(
    col("conteo_valido") == False
).count()

print("Snapshots con diferencias:", diferencias)

# Validar cantidad de registros recibidos
# en la ejecución actual.
total_leido = df.count()

total_esperado = sum(
    conteos_esperados[fecha]
    for fecha in fechas_procesadas
)

print("Total esperado en esta ejecución:", total_esperado)
print("Total leído en esta ejecución:", total_leido)

# Consultar el total histórico que permanece en Bronze.
total_almacenado = df_bronze.count()

print("Total histórico almacenado en Bronze:", total_almacenado)

# Distribución histórica por ola.
print("Distribución histórica por ola:")

df_bronze.groupBy(
    "ingestion_wave"
).count().show()

if total_leido != total_esperado:
    raise ValueError(
        "Los registros leídos no coinciden con los esperados."
    )

if diferencias > 0:
    raise ValueError(
        "Hay diferencias en los conteos almacenados."
    )

print("Proceso Bronze CRM finalizado correctamente.")

# ============================================================
# 10. VALIDACIÓN DATOS INCONSISTENTES
# ============================================================


from pyspark.sql.functions import (
    col, trim, count, countDistinct, when,
    to_date, to_timestamp, coalesce
)

print("\nVALIDACIONES DE CALIDAD CRM EN BRONZE")

df_bronze = spark.read.format("delta").load(RUTA_BRONZE)

# 1. Conteo total
print("\n1. Total de registros:")
print(df_bronze.count())

# 2. Validar registros por ola y snapshot
print("\n2. Registros por ola y snapshot:")
(
    df_bronze
    .groupBy("ingestion_wave", "snapshot_date")
    .count()
    .orderBy("ingestion_wave", "snapshot_date")
    .show(20, False)
)

# 3. Validar campos vacíos
print("\n3. Campos vacíos:")
campos = [
    "comercio_id",
    "pais",
    "documento_tributario",
    "estado",
    "fecha_afiliacion",
    "actualizado_en"
]

for campo in campos:
    cantidad = df_bronze.filter(
        col(campo).isNull() |
        (trim(col(campo)) == "")
    ).count()

    print(f"{campo}: {cantidad}")

# 4. Identificar llaves duplicadas por snapshot
print("\n4. Llaves duplicadas:")

duplicados = (
    df_bronze
    .groupBy("pais", "comercio_id", "snapshot_date")
    .count()
    .filter(col("count") > 1)
)

print("Llaves duplicadas:", duplicados.count())

print("Filas involucradas:")
duplicados.selectExpr(
    "sum(count) as filas_involucradas"
).show()

duplicados.orderBy(
    col("count").desc()
).show(20, False)

# 5. Identificar espacios al inicio o final
print("\n5. Valores con espacios externos:")

for campo in ["nombre_comercial", "ciudad"]:
    cantidad = df_bronze.filter(
        col(campo).isNotNull() &
        (col(campo) != trim(col(campo)))
    ).count()

    print(f"{campo}: {cantidad}")

# 6. Valores distintos de país y estado
print("\n6. Países encontrados:")
df_bronze.groupBy("pais").count().show()

print("\nEstados encontrados:")
df_bronze.groupBy("estado").count().show()

# 7. Validar interpretación de fechas
print("\n7. Validación de fechas:")

fecha_afiliacion = coalesce(
    to_date(col("fecha_afiliacion"), "yyyy-MM-dd"),
    to_date(col("fecha_afiliacion"), "yyyyMMdd"),
    to_date(col("fecha_afiliacion"), "dd/MM/yyyy")
)

invalidas_afiliacion = df_bronze.filter(
    col("fecha_afiliacion").isNull() |
    (trim(col("fecha_afiliacion")) == "") |
    fecha_afiliacion.isNull()
).count()

invalidos_actualizacion = df_bronze.filter(
    col("actualizado_en").isNull() |
    to_timestamp(col("actualizado_en")).isNull()
).count()

print("Fechas de afiliación inválidas:", invalidas_afiliacion)
print("Timestamps de actualización inválidos:", invalidos_actualizacion)

print("\nVALIDACIONES FINALIZADAS")

spark.stop()
