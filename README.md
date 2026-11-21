# Proyecto Pulso – Puntored

## 1. Propósito

Pipeline local tipo Medallion para consolidar transacciones de Colombia y México. La solución implementa ingesta Bronze en Delta, transformaciones Silver mediante Spark SQL, agregación Gold y exportación de las cuatro consultas solicitadas por la prueba.

> **Importante sobre el estado del código:** esta guía documenta los scripts que existen en esta versión del repositorio. No existe actualmente un `src/pulso/run.py` ni un comando único `python -m pulso.run`. La ejecución se realiza invocando los scripts `.py` en el orden indicado. Tampoco hay una carpeta `tests/` en el ZIP entregado; por tanto, `pytest -q` no representa una suite automatizada disponible en esta versión.

## 2. Requisitos del entorno

Entorno documentado para ejecución local:

- Windows 10/11 de 64 bits y PowerShell.
- Python 3.11.x.
- Java JDK 17 (requerido por PySpark).
- Git.
- Dependencias fijadas en `requirements.txt`:
  - `pyspark==3.5.5`
  - `delta-spark==3.2.1`
  - `pandas==2.2.3`
  - `pyyaml==6.0.2`
  - `python-dotenv==1.0.1`
  - `pytest==8.3.4`

`requirements-dev.txt` agrega `ruff==0.9.6`.

Verifica las versiones:

```powershell
py -3.11 --version
java -version
git --version
```

Crea y activa el entorno virtual desde la carpeta `puntored_pulso`:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Para instalar también las herramientas de desarrollo:

```powershell
pip install -r requirements-dev.txt
```

Si PowerShell impide activar el entorno:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Cierra y vuelve a abrir PowerShell, entra al proyecto y activa `.venv`.

No ejecutes `pip install -e .`: el ZIP no contiene `pyproject.toml` ni `setup.py`, por lo que no hay configuración de paquete editable.

## 3. Ubicación de los datos

Descomprime el dataset de la prueba en la raíz del proyecto, al mismo nivel de `src/`, `sql/` y `config/`. La estructura esperada por los scripts de ingesta es:

```text
puntored_pulso/
├── dataset_pulso_ESENCIAL-2026/
│   ├── ola_1/landing/
│   └── ola_2/landing/
├── config/
├── data/
├── resultados/
├── sql/
└── src/
```

**No lo ubiques dentro de `data/` para esta versión:** los scripts `bronze_colombia.py`, `bronze_mexico.py` y `bronze_comercios.py` construyen sus rutas desde la raíz del proyecto y esperan `dataset_pulso_ESENCIAL-2026/` directamente allí.

La carpeta `data/` contiene las tablas Delta generadas y debe permanecer fuera del control de versiones. No subir el dataset ni las tablas procesadas al repositorio.

## 4. Arquitectura y recorrido

```text
Dataset / landing
   │
   ├── Colombia CDC JSONL ──> Bronze co_transacciones_cdc
   ├── México CSV + TRL ─────> Bronze mx_transacciones
   ├── CRM snapshots ────────> Bronze crm_comercios
   └── Catálogos ────────────> Bronze catalogos_*
                                  │
                                  ├── Validación calidad México / TRL
                                  │
                                  ▼
                         Silver SQL (4 scripts SQL)
                         ├── transacciones Colombia
                         ├── transacciones México
                         ├── consolidación CO + MX
                         └── CRM vigente + cuarentena
                                  │
                                  ▼
                         Gold ventas_diarias
                                  │
                                  ▼
                         CSV Gold + consultas SQL CSV
```

### Directorios generados

- `data/lakehouse/bronze/`: tablas Bronze por fuente.
- `data/lakehouse/bronze/quarantine/`: cuarentena de México.
- `data/lakehouse/bronze/quality/`: hallazgos y validación de TRL.
- `data/lakehouse/silver/`: transacciones consolidadas, CRM vigente y cuarentena CRM.
- `data/lakehouse/gold/`: agregado de ventas diarias.
- `resultados/`: exportaciones CSV finales.

## 5. Orden exacto de ejecución

Ejecuta los comandos desde la raíz `puntored_pulso`, con `.venv` activado. Cada comando debe terminar correctamente antes de pasar al siguiente.

### Paso 1. Ingesta de catálogos a Bronze

```powershell
python src/ingesta/bronze_catalogos.py
```

Carga productos, canales y taxonomía. En el código actual `OLA = "ola_1"` y `CATALOG_VERSION = "2026-06-01"` están definidos directamente; no recibe parámetro de ola. Se ejecuta una vez como carga de catálogos de referencia.

### Paso 2. Ingesta de transacciones Colombia a Bronze

Procesar únicamente ola 1:

```powershell
python src/ingesta/bronze_colombia.py --ola ola_1
```

Procesar únicamente ola 2:

```powershell
python src/ingesta/bronze_colombia.py --ola ola_2
```

Procesar las dos olas en una misma ejecución:

```powershell
python src/ingesta/bronze_colombia.py --ola ambas
```

El parámetro acepta `ola_1`, `ola_2` o `ambas`; por defecto es `ambas`. El script calcula checksum e identificador de ingesta por archivo y compara con los `ingestion_file_id` ya presentes en Bronze. Solo incorpora archivos nuevos. La escritura final se hace en Delta con `append`; la protección contra reingesta se implementa mediante la identificación de archivos procesados, no mediante una instrucción Delta `MERGE` explícita.

### Paso 3. Ingesta de transacciones México a Bronze

Ola 1:

```powershell
python src/ingesta/bronze_mexico.py ola_1
```

Ola 2:

```powershell
python src/ingesta/bronze_mexico.py ola_2
```

El script recibe una sola ola como argumento posicional. Clasifica filas de transacción y registros de control `TRL`, agrega metadatos de archivo/ola y evita volver a cargar archivos cuyo `source_file` ya existe en Bronze. La escritura es `append` de los archivos nuevos.

Si necesitas ambas olas, ejecuta el script dos veces, una por ola. No uses `ambas` en este script: no es un valor admitido.

### Paso 4. Ingesta de CRM a Bronze

```powershell
python src/ingesta/bronze_comercios.py
```

Este script **no recibe parámetro de ola**: tiene `OLAS = ["ola_1", "ola_2"]` definido en el código y recorre los snapshots CRM de ambas olas. Reúne los archivos `snapshot_fecha=*/comercios.csv` y actualiza Bronze por snapshots procesados.

**Limitación operativa importante:** si se incorpora una nueva ola o una nueva ubicación/fuente CRM, no basta con pasar un argumento en la línea de comandos. Se debe ajustar el código (`OLAS` y/o `RUTA_BASE` y el patrón de lectura) y validar el mecanismo de reemplazo de snapshots antes de ejecutar. El descubrimiento dinámico de nuevas olas CRM no está parametrizado en esta versión.

### Paso 5. Validación de calidad de México y registros TRL

```powershell
python src/calidad/validar_calidad_mexico.py
```

Lee Bronze México, separa transacciones y controles, evalúa montos y genera cuarentena, hallazgos y validación del registro de control por archivo. Revisa en la salida la comparación de conteo y suma declarados en el `TRL` frente a los valores leídos. Los resultados se guardan bajo `data/lakehouse/bronze/quality/` y la cuarentena bajo `data/lakehouse/bronze/quarantine/`.

No continúes si el script termina con error. Investiga el archivo y el motivo reportado; no elimines controles ni registros de forma manual para forzar la ejecución.

### Paso 6. Transformaciones Silver

```powershell
python src/transformaciones/ejecutar_silver_sql.py
```

Este es el orquestador disponible para Silver. Antes de transformar, valida la existencia de las tablas Bronze requeridas y registra las vistas correspondientes. Ejecuta en este orden:

1. `sql/silver/01_transacciones_colombia.sql`
2. `sql/silver/02_transacciones_mexico.sql`
3. `sql/silver/03_consolidar_transacciones.sql`
4. `sql/silver/04_crm_comercios.sql`

Genera y valida las tablas Silver de transacciones consolidadas, CRM vigente y cuarentena CRM. También imprime controles de duplicados, llaves nulas, estados, homologaciones y conteos.

**Dependencia:** deben existir previamente Bronze Colombia, México, CRM y los tres catálogos. La validación de calidad México debe haberse ejecutado para que estén disponibles las tablas de calidad/TRL que consume Silver.

### Paso 7. Validación adicional de Silver

```powershell
python src/pulso/validar_silver.py
```

Realiza controles adicionales sobre la tabla Silver de transacciones, como volumen, distribución por país/estado y duplicados de llave. Es un script de validación independiente, no transforma la capa.

### Paso 8. Construcción de Gold

```powershell
python src/transformaciones/ejecutar_gold_sql.py
```

Lee `data/lakehouse/silver/transacciones_sql/`, registra la vista de Silver, ejecuta `sql/gold/01_ventas_diarias.sql`, escribe la tabla Delta Gold en `data/lakehouse/gold/ventas_diarias/` y realiza conciliaciones con `sql/gold/02_validar_ventas_diarias.sql`.

La tabla Gold tiene grano `fecha_negocio + pais + linea_negocio_corporativa`. Incluye transacciones efectivas, valor efectivo local, ingreso por comisión, reversadas y rechazadas.

### Paso 9. Exportación de la tabla Gold

El script Gold genera la tabla Delta. El archivo CSV de salida solicitado se encuentra en `resultados/gold_ventas_diarias.csv` en la entrega. Si se requiere regenerarlo desde la tabla Delta, exporta el resultado Gold según el bloque de exportación del propio `ejecutar_gold_sql.py`; verifica fecha de modificación y contenido del CSV después de una nueva corrida.

### Paso 10. Ejecución y exportación de consultas SQL

```powershell
python src/pulso/exportacion_resultado_consultas_sql.py
```

Registra como vistas las tablas Delta `silver_consolidada` y `silver_crm_comercios`, ejecuta los cuatro archivos de `sql/consultas/` y exporta un CSV por consulta a `resultados/consultas_sql/`.

| Archivo SQL | Pregunta que responde | CSV generado |
|---|---|---|
| `01_top_10_comercios_julio.sql` | Top 10 comercios por ingreso de comisión en julio, separado por país, con segmento y ciudad | `01_top_10_comercios_julio.csv` |
| `02_tasa_reverso.sql` | Tasa de reverso por país y línea corporativa | `02_tasa_reverso.csv` |
| `03_mayor_caida_transacciones.sql` | Fecha con mayor caída frente al promedio de los demás días de la misma semana | `03_mayor_caida_transacciones.csv` |
| `04_comercios_sin_transacciones_julio.sql` | Comercios activos en CRM sin transacciones durante julio | `04_comercios_sin_transacciones_julio.csv` |

Las consultas se ejecutan contra Silver, no contra Gold. El script de exportación requiere que existan tanto Silver consolidada como CRM vigente.

## 6. Procesamiento de olas e idempotencia: qué hace realmente esta versión

El diseño permite seleccionar la ola de transacciones de Colombia y México y evita incorporar nuevamente archivos ya procesados:

| Fuente | Cómo se selecciona la ola | Protección de reingesta en el código |
|---|---|---|
| Colombia | `--ola ola_1`, `--ola ola_2` o `--ola ambas` | `ingestion_file_id` basado en ruta relativa + checksum; filtra archivos ya registrados |
| México | argumento posicional `ola_1` o `ola_2` | `left_anti` por `source_file`; agrega solo archivos no presentes |
| CRM | Sin argumento; procesa `ola_1` y `ola_2` definidas en el código | Reemplaza/actualiza snapshots procesados en Bronze |
| Catálogos | Sin argumento; fija ola 1 y versión en el código | Escritura controlada por `replaceWhere` en las tablas Delta |

En conversación suele llamarse “merge” al comportamiento de integrar incrementalmente las olas sobre el histórico existente. Sin embargo, en los scripts de ingesta de esta versión no aparece una operación `DeltaTable.merge()` / SQL `MERGE INTO`: Colombia y México identifican archivos nuevos y escriben `append`; CRM actualiza snapshots mediante overwrite condicionado. Por eso, la garantía concreta depende de la identidad del archivo y de las condiciones implementadas por cada fuente.

Para una ejecución desde cero, procesa ola 1 y después ola 2 en Colombia y México. Para reejecutar una ola ya procesada, la identificación de archivos debe evitar su duplicación. El resultado Silver/Gold debe validarse nuevamente después de cualquier carga nueva.

## 7. Reejecución y limpieza

- **Reejecutar Colombia:** puedes volver a lanzar el mismo parámetro; los archivos ya registrados se reconocen por `ingestion_file_id`.
- **Reejecutar México:** el filtro por `source_file` evita anexar de nuevo los archivos ya presentes.
- **Reejecutar CRM:** el script vuelve a recorrer los snapshots de ambas olas y aplica su lógica de reemplazo de snapshots.
- **Reejecutar Silver/Gold:** son transformaciones que escriben tablas con `overwrite`; recalculan la salida a partir de Bronze/Silver disponible.
- **No borres manualmente `data/lakehouse/` para resolver errores** sin respaldar primero y entender la causa. Borrar Bronze elimina el registro de archivos ya ingeridos y cambia el punto de partida.

## 8. Consultas y definición de métricas

- **Transacción efectiva:** estado `APROBADA`.
- **Ingreso:** suma de `comision` para transacciones aprobadas, en moneda local.
- **Reverso:** estado homologado `REVERSADA`.
- **Tasa de reverso implementada:** `reversadas / (aprobadas + reversadas) * 100`, por país y línea corporativa; excluye registros con observación de calidad, fechas nulas y línea no homologada.
- **Top 10:** suma comisión de transacciones aprobadas en julio, ranking independiente por país; utiliza atributos del CRM marcado como versión actual.
- **Mayor caída:** compara el total diario contra el promedio de los otros días disponibles con el mismo `DAYOFWEEK`; la consulta devuelve la mayor caída absoluta.
- **CRM sin transacciones:** considera comercios `ACTIVO` en la versión actual del CRM y ausencia de transacciones de julio.

## 9. Configuración y portabilidad

Existen `config/local.yaml` y `config/databricks.yaml`, pero **los scripts actuales no cargan esos YAML de forma centralizada**: varias rutas y parámetros Spark están definidos dentro de cada `.py`. Además, `config/local.yaml` describe `data/dataset_pulso_ESENCIAL-2026`, mientras que los scripts de ingesta esperan el dataset en la raíz del proyecto.

Para llevar la solución a Databricks se requiere, como mínimo:

1. Sustituir rutas locales por rutas de landing/volúmenes o ubicaciones S3/Unity Catalog.
2. Unificar la lectura de configuración para que los YAML sean realmente la fuente de rutas y parámetros.
3. Usar el runtime Spark/Delta provisto por el workspace y validar compatibilidad; no instalar `delta-spark` a ciegas sobre el runtime.
4. Orquestar los pasos como Jobs/Workflows con dependencias y parámetros por ola.
5. Configurar permisos, secretos y acceso a datos mediante mecanismos del workspace; no almacenar credenciales en archivos.
6. Mantener sin cambios las reglas de negocio, las transformaciones SQL y las validaciones siempre que el entorno destino soporte las mismas funciones.

## 10. Salidas esperadas

Al terminar correctamente, verifica:

```text
resultados/
├── gold_ventas_diarias.csv
└── consultas_sql/
    ├── 01_top_10_comercios_julio.csv
    ├── 02_tasa_reverso.csv
    ├── 03_mayor_caida_transacciones.csv
    └── 04_comercios_sin_transacciones_julio.csv
```

El ZIP entregado contiene estos CSV como resultados de referencia. Después de una nueva ejecución, confirma que sus fechas de modificación y contenidos correspondan a la corrida actual.

## 11. Solución de problemas frecuentes

- **No encuentra `dataset_pulso_ESENCIAL-2026`:** confirma que la carpeta está en la raíz del proyecto, no dentro de `data/`.
- **Error de Java/PySpark:** valida JDK 17, `JAVA_HOME` y que el entorno virtual esté activo.
- **No encuentra una tabla Delta:** ejecuta las ingestas previas en el orden indicado y confirma que la carpeta contiene `_delta_log`.
- **México no acepta `ambas`:** el script solo admite `ola_1` u `ola_2`; ejecútalo dos veces.
- **CRM no incorpora un nuevo origen:** modifica `OLAS`/rutas/patrón de lectura en `bronze_comercios.py`; no existe parámetro dinámico.
- **Error de calidad o TRL:** revisa el archivo reportado, conteo y suma; conserva el registro original y documenta la decisión.
- **Consultas sin vista o tabla:** ejecuta Silver y Gold antes del exportador de consultas; las consultas necesitan Silver consolidada y CRM vigente.

## 12. Alcance real de esta entrega

Esta guía no afirma que exista un orquestador único, una suite de pruebas automatizadas, una ingesta CRM parametrizada por ola ni una instrucción Delta `MERGE` en los scripts de ingesta. Esos elementos no aparecen implementados en el ZIP inspeccionado. El pipeline se reproduce mediante los comandos secuenciales descritos arriba.


## 13. Reglas de calidad y publicación (detalle de la ejecución final)

La capa de calidad México es un paso obligatorio entre Bronze y Silver. No basta con crear cuarentena: Silver registra y aplica las decisiones de publicación por registro y por archivo.

### 13.1 Salidas de calidad

`src/calidad/validar_calidad_mexico.py` genera tres resultados Delta:

| Salida | Contenido | Uso posterior |
|---|---|---|
| `data/lakehouse/bronze/quarantine/mx_transacciones` | Registros individuales con monto nulo o no interpretable y su motivo | Silver los excluye por `source_file` + `ID_OPERACION` |
| `data/lakehouse/bronze/quality/hallazgos_calidad` | Resumen de reglas, fuente, cantidad, severidad y decisión | Auditoría y seguimiento |
| `data/lakehouse/bronze/quality/validacion_trl_mexico` | Validación de conteo/suma TRL y estado de publicación por archivo | Silver excluye todos los registros de archivos `BLOQUEADO` |

### 13.2 Clasificación de archivos

- `APTO`: pasa las validaciones de archivo y puede publicarse.
- `APTO_PARCIAL`: presenta problemas a nivel de registros individuales; puede publicarse excluyendo los registros en cuarentena.
- `BLOQUEADO`: falla un control de integridad del archivo (por ejemplo TRL ausente, duplicado o diferencia de conteo/suma); ninguna transacción de ese archivo pasa a Silver.

En la ejecución documentada se evaluaron 61 archivos: 28 `APTO`, 32 `APTO_PARCIAL` y 1 `BLOQUEADO`.

Caso bloqueado: `trx_mx_20260722.csv`. El TRL declara 793 transacciones y 288.390,50; el archivo contiene 570 transacciones y 176.762,50. La diferencia es de 223 transacciones y 111.628,00. Se excluye el archivo completo de Silver, pero se conserva íntegro en Bronze.

### 13.3 Orden real dentro de Silver México

El SQL `sql/silver/02_transacciones_mexico.sql` aplica la siguiente secuencia:

1. Identifica los archivos con `estado_publicacion = 'BLOQUEADO'`.
2. Lee registros `TRANSACCION` y excluye filas sin `ID_OPERACION`.
3. Calcula `fecha_hora_negocio` y `fecha_actualizacion`.
4. Selecciona la última versión por `ID_OPERACION` con `ROW_NUMBER()`, ordenando por fecha de actualización y metadatos de carga.
5. Excluye operaciones individuales de `quarantine_mx` mediante `LEFT ANTI JOIN` por `source_file` + `ID_OPERACION`.
6. Excluye todas las operaciones de archivos bloqueados mediante `LEFT ANTI JOIN` por `source_file`.
7. Convierte montos y comisiones a `DECIMAL(18,2)`, normaliza canal/estado y homologa `CANCELADA` como `REVERSADA`.

**Nota:** el SQL selecciona la última versión antes de aplicar la cuarentena por archivo + operación. No se debe interpretar que los duplicados se borran en Bronze; se conserva la evidencia y Silver decide qué versión publicar.

### 13.4 Reglas adicionales de calidad en la consolidación

`sql/silver/03_consolidar_transacciones.sql` agrega indicadores de calidad a la transacción consolidada:

- estado válido;
- fecha de negocio válida;
- monto y comisión válidos;
- canal homologado;
- producto homologado;
- línea corporativa homologada;
- monto dentro del rango del producto;
- producto de prueba (`TST-001` en Colombia).

Asigna una `observacion_calidad` prioritaria por registro. En la ejecución documentada se reportaron:

| Hallazgo prioritario | Registros |
|---|---:|
| `COMISION_INVALIDA` | 251 |
| `MONTO_FUERA_DE_RANGO_PRODUCTO` | 28 |
| `PRODUCTO_NO_HOMOLOGADO` | 139 |
| Sin observación prioritaria | 131.724 |

Un registro con más de una anomalía puede quedar representado solo por la primera observación según la prioridad del `CASE`. Los indicadores booleanos permiten revisar otras condiciones sin confundir el conteo de observación prioritaria con el total de anomalías.

El producto `TST-001` se marca como producto de prueba. La documentación funcional indica que los productos/comercios de prueba no deben participar en cifras oficiales. Antes de publicar métricas, verifica que el filtro de Gold excluya `es_producto_prueba` y que la definición de transacción efectiva y reverso se aplique consistentemente. No basta con marcarlo si el SQL de Gold no lo filtra.

## 14. Evidencia y conciliación de Silver

La ejecución final documentada produjo:

| Indicador | Resultado |
|---|---:|
| Transacciones Colombia Silver | 80.282 |
| Transacciones México Silver | 51.860 |
| Total consolidado | 132.142 |
| Llaves nulas CO / MX | 0 / 0 |
| Llaves duplicadas CO / MX | 0 / 0 |
| Estados nulos | 0 |
| Canales sin homologación | 0 |

Distribución de estados:

| País | Aprobadas | Rechazadas | Pendientes | Reversadas |
|---|---:|---:|---:|---:|
| CO | 70.035 | 9.363 | 85 | 799 |
| MX | 45.749 | 4.668 | 1.015 | 428 |

La salida consolidada Delta está en `data/lakehouse/silver/transacciones_sql`. Los valores son evidencia de la corrida documentada; una nueva ingesta puede cambiar los resultados.

## 15. CRM y catálogos: reglas que deben preservarse

- CRM: 326 registros involucrados en duplicados (163 grupos); deduplicar en Silver por `pais + comercio_id + snapshot_date`, sin sobrescribir la historia de snapshots en Bronze.
- CRM: aplicar `TRIM` a `nombre_comercial` y `ciudad`; normalizar los tres formatos de `fecha_afiliacion` a `DATE`. Conservar estados `ACTIVO`, `INACTIVO` y `SUSPENDIDO`.
- Productos: `sku_origen` puede ser nulo (18 registros); no usarlo como llave obligatoria. La llave funcional de referencia es `pais + producto_cod`.
- Productos: `vigente_hasta` nulo se interpreta como vigencia abierta, no como error automático.
- Productos: homologar las 12 etiquetas de línea de origen contra las 7 líneas corporativas en Silver, manteniendo el valor original.
- Canales y taxonomía: no se reportan nulos ni llaves duplicadas en el inventario de calidad; conservar códigos originales y homologar por país + código.

## 16. Ejecución por olas: recomendación operativa

Las notas funcionales definen `ola_1` (llegadas del 1 al 30 de junio de 2026) y `ola_2` (llegadas del 1 de julio al 2 de agosto de 2026). Para una corrida controlada y auditable, procesa primero ola 1 y luego ola 2, aunque el script Colombia permita `--ola ambas`.

Ejemplo recomendado:

```powershell
# Colombia: primero ola 1, luego ola 2
python src/ingesta/bronze_colombia.py --ola ola_1
python src/ingesta/bronze_colombia.py --ola ola_2

# México: el script admite una ola por ejecución
python src/ingesta/bronze_mexico.py ola_1
python src/ingesta/bronze_mexico.py ola_2

# CRM: no admite parámetro; procesa las olas definidas en el código
python src/ingesta/bronze_comercios.py

# Después de tener Bronze de ambas olas:
python src/calidad/validar_calidad_mexico.py
python src/transformaciones/ejecutar_silver_sql.py
python src/transformaciones/ejecutar_gold_sql.py
python src/pulso/exportacion_resultado_consultas_sql.py
```

Si se necesita probar solo una ola, ejecuta la ingesta de esa ola y valida que las tablas de referencia necesarias estén presentes. La calidad México se vuelve a ejecutar después de cada nueva carga México y antes de Silver, para que las decisiones TRL/cuarentena correspondan al Bronze actual.
