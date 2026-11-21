from pathlib import Path
import sys
import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CONFIGURACION DE RUTAS
# ============================================================

PROYECTO = Path(__file__).resolve().parents[2]

RUTA_SILVER = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "silver"
    / "transacciones_sql"
)

RUTA_GOLD = (
    PROYECTO
    / "data"
    / "lakehouse"
    / "gold"
    / "ventas_diarias"
)

RUTA_RESULTADOS = PROYECTO / "resultados"

RUTA_SQL_GOLD = (
    PROYECTO
    / "sql"
    / "gold"
    / "01_ventas_diarias.sql"
)

RUTA_RESULTADOS.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. VALIDAR EXISTENCIA DE RUTAS
# ============================================================

if not RUTA_SILVER.exists():
    raise FileNotFoundError(
        f"No existe la tabla Silver: {RUTA_SILVER}. "
        "Ejecuta primero ejecutar_silver_sql.py"
    )

if not RUTA_SQL_GOLD.exists():
    raise FileNotFoundError(
        f"No existe el SQL Gold: {RUTA_SQL_GOLD}"
    )


# ============================================================
# 3. CONFIGURAR SPARK + DELTA
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
    .appName("Gold_Ventas_Diarias_Pulso")
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


try:

    # ========================================================
    # 4. REGISTRAR SILVER COMO VISTA TEMPORAL
    # ========================================================

    print("\n========== CARGA SILVER ==========")

    df_silver = (
        spark.read
        .format("delta")
        .load(str(RUTA_SILVER))
    )

    df_silver.createOrReplaceTempView("silver_consolidada")

    print("Registros Silver:", df_silver.count())
    print("Vista silver_consolidada registrada correctamente.")

    print("\nEsquema Silver:")
    df_silver.printSchema()


    # ========================================================
    # 5. EJECUTAR SQL GOLD
    # ========================================================

    print("\n========== TRANSFORMACION GOLD ==========")

    contenido_sql = RUTA_SQL_GOLD.read_text(
        encoding="utf-8"
    )

    sentencias = [
        sentencia.strip()
        for sentencia in contenido_sql.split(";")
        if sentencia.strip()
    ]

    for i, sentencia in enumerate(sentencias, start=1):

        print(f"Ejecutando sentencia {i}/{len(sentencias)}")

        spark.sql(sentencia)

    print("SQL Gold ejecutado correctamente.")


    # ========================================================
    # 6. VALIDAR RESULTADO GOLD
    # ========================================================

    print("\n========== VALIDACION GOLD VENTAS DIARIAS ==========")

    df_gold = spark.table("ventas_diarias")


    # ============================================================
    # VALIDACIONES DE CALIDAD Y CONCILIACION DE GOLD
    # ============================================================

    print("\n========== VALIDACIONES GOLD ==========")

    # 1. Validar campos obligatorios de la llave de negocio
    campos_clave = [
        "fecha_negocio",
        "pais",
        "linea_negocio_corporativa"
    ]

    condicion_nulos = None

    for campo in campos_clave:
        condicion = F.col(campo).isNull()
        condicion_nulos = (
            condicion if condicion_nulos is None
            else condicion_nulos | condicion
        )

    registros_sin_clave = df_gold.filter(condicion_nulos).count()

    print(f"Registros con campos clave nulos: {registros_sin_clave}")

    if registros_sin_clave > 0:
        raise ValueError("Gold contiene registros con campos clave nulos.")


    # 2. Validar que no existan duplicados en el grano de Gold
    duplicados = (
        df_gold
        .groupBy(
            "fecha_negocio",
            "pais",
            "linea_negocio_corporativa"
        )
        .count()
        .filter(F.col("count") > 1)
    )

    cantidad_duplicados = duplicados.count()

    print(f"Claves duplicadas en Gold: {cantidad_duplicados}")

    if cantidad_duplicados > 0:
        duplicados.show(truncate=False)
        raise ValueError("Gold contiene duplicados en su grano de negocio.")


    # 3. Validar países permitidos
    paises_invalidos = (
        df_gold
        .filter(~F.col("pais").isin("CO", "MX"))
        .count()
    )

    print(f"Registros con país inválido: {paises_invalidos}")

    if paises_invalidos > 0:
        raise ValueError("Gold contiene países diferentes de CO y MX.")


    # 4. Validar rango de fechas generado
    rango_fechas = df_gold.agg(
        F.min("fecha_negocio").alias("fecha_minima"),
        F.max("fecha_negocio").alias("fecha_maxima")
    ).first()

    print(f"Fecha mínima: {rango_fechas['fecha_minima']}")
    print(f"Fecha máxima: {rango_fechas['fecha_maxima']}")


    # 5. Conciliar cantidades entre Silver y Gold
    # Se utiliza exactamente el mismo filtro de la consulta Gold.

    silver_validado = df_silver.filter(
        F.col("fecha_negocio").isNotNull()
        & F.col("pais").isin("CO", "MX")
        & F.col("linea_negocio_corporativa").isNotNull()
        & F.col("observacion_calidad").isNull()
        & F.col("estado").isin(
            "APROBADA",
            "REVERSADA",
            "RECHAZADA"
        )
    )

    conteos_silver = {
        fila["estado"]: fila["count"]
        for fila in (
            silver_validado
            .groupBy("estado")
            .count()
            .collect()
        )
    }

    totales_gold = df_gold.agg(
        F.sum("trx_efectivas").alias("aprobadas"),
        F.sum("trx_reversadas").alias("reversadas"),
        F.sum("trx_rechazadas").alias("rechazadas")
    ).first()

    comparacion = [
        (
            "APROBADA",
            conteos_silver.get("APROBADA", 0),
            int(totales_gold["aprobadas"] or 0)
        ),
        (
            "REVERSADA",
            conteos_silver.get("REVERSADA", 0),
            int(totales_gold["reversadas"] or 0)
        ),
        (
            "RECHAZADA",
            conteos_silver.get("RECHAZADA", 0),
            int(totales_gold["rechazadas"] or 0)
        )
    ]

    print("\nConciliación Silver vs Gold:")

    errores_conciliacion = []

    for estado, total_silver, total_gold in comparacion:
        coincide = total_silver == total_gold

        print(
            f"{estado}: Silver={total_silver} | "
            f"Gold={total_gold} | "
            f"Coincide={coincide}"
        )

        if not coincide:
            errores_conciliacion.append(estado)

    if errores_conciliacion:
        raise ValueError(
            "Diferencias entre Silver y Gold para los estados: "
            + ", ".join(errores_conciliacion)
        )


    # 6. Resumen de registros Gold
    print(f"\nTotal de filas Gold: {df_gold.count()}")
    print("Todas las validaciones Gold finalizaron correctamente.")
    print("=======================================\n")


    total_gold = df_gold.count()

    print("Registros Gold:", total_gold)

    df_gold.orderBy(
        "fecha_negocio",
        "pais",
        "linea_negocio_corporativa"
    ).show(100, truncate=False)


    # ========================================================
    # 7. VALIDACIONES DE CALIDAD GOLD
    # ========================================================

    duplicados = (
        df_gold
        .groupBy(
            "fecha_negocio",
            "pais",
            "linea_negocio_corporativa"
        )
        .count()
        .filter("count > 1")
        .count()
    )

    llaves_nulas = df_gold.filter(
        """
        fecha_negocio IS NULL
        OR pais IS NULL
        OR linea_negocio_corporativa IS NULL
        """
    ).count()

    print("Duplicados Gold:", duplicados)
    print("Llaves nulas Gold:", llaves_nulas)

    if duplicados > 0:
        raise ValueError(
            "Gold contiene duplicados en su grano."
        )

    if llaves_nulas > 0:
        raise ValueError(
            "Gold contiene llaves nulas."
        )


    # ========================================================
    # 8. GUARDAR GOLD EN DELTA
    # ========================================================

    print("\n========== ESCRITURA GOLD ==========")

    (
        df_gold.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(RUTA_GOLD))
    )

    print("Tabla Gold almacenada correctamente.")


    # ========================================================
    # 9. VERIFICAR PERSISTENCIA
    # ========================================================

    df_gold_guardado = (
        spark.read
        .format("delta")
        .load(str(RUTA_GOLD))
    )

    total_guardado = df_gold_guardado.count()

    print("\n========== RESULTADO FINAL GOLD ==========")

    print("Registros generados:", total_gold)
    print("Registros almacenados:", total_guardado)

    if total_gold != total_guardado:
        raise ValueError(
            "La cantidad de registros Gold no coincide."
        )


    # ==========================================
    # CONCILIACION SILVER VS GOLD
    # ==========================================

    print("\n========== CONCILIACION SILVER VS GOLD ==========")

    ruta_validacion = (
        PROYECTO / "sql" / "gold" / "02_validar_ventas_diarias.sql"
    )

    sql_validacion = ruta_validacion.read_text(encoding="utf-8")

    resultado_conciliacion = spark.sql(sql_validacion)

    diferencias = resultado_conciliacion.count()

    if diferencias == 0:
        print("CONCILIACION EXITOSA: Silver y Gold coinciden.")
    else:
        print(f"CONCILIACION FALLIDA: Se encontraron {diferencias} diferencias.")
        resultado_conciliacion.show(100, truncate=False)
        raise ValueError("Gold no coincide con los datos de Silver.")

    # ========================================================
    # 10. EXPORTAR CSV
    # ========================================================

    ruta_csv = RUTA_RESULTADOS / "gold_ventas_diarias.csv"

    (
        df_gold_guardado
        .toPandas()
        .to_csv(
            ruta_csv,
            index=False,
            encoding="utf-8-sig"
        )
    )

    print("\nGold guardado:", RUTA_GOLD)
    print("CSV generado:", ruta_csv)

    print("\nTransformacion Gold finalizada correctamente.")


finally:

    spark.stop()
