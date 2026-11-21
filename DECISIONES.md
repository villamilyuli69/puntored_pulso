# DECISIONES.md – Proyecto Pulso

## 1. Propósito

Registrar las decisiones técnicas y de negocio de la versión final del proyecto, sustentadas en los scripts Python, SQL, resultados de calidad y conciliaciones documentadas. La solución procesa fuentes de Colombia y México, CRM y catálogos con una arquitectura Medallion local basada en PySpark, Spark SQL y Delta Lake.

La documentación distingue entre: (1) reglas efectivamente implementadas, (2) resultados observados en la corrida documentada y (3) recomendaciones o pendientes. Esta separación evita atribuir al código comportamientos que todavía no están implementados.

## 2. Arquitectura y responsabilidad de las capas

| Capa | Responsabilidad | Resultado |
|---|---|---|
| Landing | Archivos originales organizados por fuente y ola | JSONL Colombia, CSV México/CRM y catálogos |
| Bronze | Conservación del origen y metadatos de ingesta en Delta | Tablas Bronze por fuente |
| Quality | Validación por registro y por archivo, cuarentena, hallazgos y control TRL | Tablas Delta de calidad |
| Silver | Tipificación, deduplicación, selección de estado/versión, homologación y consolidación | `transacciones_sql`, CRM vigente y cuarentena CRM |
| Gold | Métricas agregadas para Finanzas | `ventas_diarias` |
| Consultas | Respuestas analíticas solicitadas y exportación | Cuatro CSV en `resultados/consultas_sql/` |

Las transformaciones conservan el dato original en Bronze. Los ajustes y exclusiones se aplican en Quality/Silver, de modo que una anomalía no implique borrar la evidencia de origen.

## 3. Decisiones de ingesta

### 3.1 Tratamiento de las olas

Las olas son llegadas sucesivas: `ola_1` cubre archivos recibidos entre 2026-06-01 y 2026-06-30; `ola_2`, entre 2026-07-01 y 2026-08-02. La recomendación operativa es procesarlas en ese orden para facilitar la trazabilidad, aunque Colombia permite ejecutar ambas en una sola invocación.

- **Colombia:** `bronze_colombia.py` acepta `--ola ola_1`, `--ola ola_2` o `--ola ambas`. Genera un checksum SHA-256 por archivo e `ingestion_file_id` determinístico usando ruta relativa + checksum; compara contra IDs persistidos y procesa archivos nuevos. La escritura es incremental.
- **México:** `bronze_mexico.py` acepta una ola por ejecución (`ola_1` u `ola_2`). Conserva los 11 campos originales como texto, clasifica filas `TRL` como controles y agrega `source_file`, fecha de carga, timestamp y ola. Evita volver a incorporar archivos cuyo `source_file` ya exista.
- **CRM:** `bronze_comercios.py` define `OLAS = ["ola_1", "ola_2"]` en código. No tiene parámetro `--ola`; para incluir otra ola/ruta se debe modificar la lista y, si aplica, la ruta o patrón de lectura.
- **Catálogos:** el script fija la ola y versión de catálogo en código; conserva los valores de origen y aplica reemplazo controlado para la versión cargada.

**Precisión sobre MERGE:** el requerimiento operativo de combinar las olas se resuelve mediante ingesta incremental sobre las tablas Bronze existentes. Sin embargo, los scripts de Colombia y México no ejecutan un `DeltaTable.merge()` ni `MERGE INTO` por llave de negocio; su control de reingesta es por archivo y la persistencia incorpora los archivos nuevos. Por eso, técnicamente debe describirse como *integración incremental de olas con control de archivos*, no como MERGE transaccional de filas. Esta distinción importa para el tratamiento de archivos corregidos: México identifica por `source_file`, de modo que un archivo modificado conservando exactamente la misma ruta no se vuelve a ingerir automáticamente.

### 3.2 Bronze: conservar antes de corregir

Se eligió Delta Lake para Bronze por su integración con Spark/Databricks, consistencia transaccional y capacidad de mantener versiones. Se preservan campos de origen y metadatos para rastrear archivo, ola y procesamiento. En Colombia se conservan eventos CDC completos (before/after/op y metadatos Kafka); en México se conservan los campos originales y también las filas TRL; CRM conserva los snapshots y sus fechas.

No se deduplica por llave de negocio en Bronze. Una transacción colombiana puede tener varios eventos CDC y una operación mexicana puede aparecer en más de una versión. La selección de estado/versión corresponde a Silver.

## 4. Decisiones de calidad de datos

### 4.1 Principio general

La calidad se ejecuta sobre Bronze y produce tres tipos de evidencia: registros individuales en cuarentena, resumen de hallazgos y validación de integridad del archivo. Una regla individual no debe confundirse con una regla que bloquea todo un archivo.

### 4.2 México: cuarentena y control TRL

`src/calidad/validar_calidad_mexico.py` implementa:

- detección de `MONTO` nulo o vacío;
- detección de monto no interpretable;
- separación de transacciones y filas de control `TRL`;
- comparación por archivo del conteo y suma de montos calculados contra los valores declarados en TRL;
- clasificación del archivo según su aptitud de publicación.

Genera estas tablas Delta:

| Salida | Función |
|---|---|
| `bronze/quarantine/mx_transacciones` | Filas individuales que no deben pasar a Silver, con su motivo |
| `bronze/quality/hallazgos_calidad` | Resumen de regla, archivo/fuente, cantidad, severidad y decisión |
| `bronze/quality/validacion_trl_mexico` | Conteo/suma declarados y calculados y estado de publicación por archivo |

Estados de publicación:

- `APTO`: supera las validaciones del archivo; puede continuar a Silver.
- `APTO_PARCIAL`: hay observaciones individuales; el archivo continúa excluyendo las filas en cuarentena.
- `BLOQUEADO`: el control de integridad no permite certificar el archivo; ninguna transacción del archivo continúa a Silver.

**Caso de archivo bloqueado:** `trx_mx_20260722.csv` declara en TRL 793 transacciones y 288.390,50; se identificaron 570 transacciones y 176.762,50. La diferencia es 223 transacciones y 111.628,00. La decisión es bloquear las 570 transacciones de ese archivo para Silver, conservándolo íntegramente en Bronze.

**Resultados de clasificación:** 61 archivos evaluados: 28 `APTO` (45,9 %), 32 `APTO_PARCIAL` (52,5 %) y 1 `BLOQUEADO` (1,6 %).

**Cuarentena:** el informe final de ejecución Silver/Quality documenta 48 registros `MONTO_NULO` y 0 `MONTO_INVALIDO`. El inventario de hallazgos complementario registra 25 valores `MONTO` vacíos. Son cifras de artefactos/cortes distintos y no deben sumarse ni presentarse como una misma corrida. Para describir la ejecución final del pipeline se toma como referencia el resultado integrado del documento de transformaciones: 48 en cuarentena.

### 4.3 Cómo Quality controla Silver

En `sql/silver/02_transacciones_mexico.sql`:

1. Se identifican los archivos `BLOQUEADO` desde `validacion_trl_mexico`.
2. Se leen únicamente filas clasificadas como transacciones con `ID_OPERACION` informado.
3. Se calculan fecha de negocio y fecha de actualización.
4. Se selecciona la última versión por `ID_OPERACION` mediante `ROW_NUMBER()`, ordenando por `fecha_actualizacion`, fecha de carga, timestamp y archivo.
5. Se excluyen las operaciones en cuarentena con `LEFT ANTI JOIN` por `source_file + ID_OPERACION`.
6. Se excluyen todos los registros de archivos bloqueados con otro `LEFT ANTI JOIN` por `source_file`.
7. Se convierten montos/comisiones a `DECIMAL(18,2)`, se normaliza canal y se homologa `CANCELADA` a `REVERSADA`.

El `LEFT ANTI JOIN` conserva solo filas de la izquierda sin coincidencia en el conjunto de exclusión. Así, la cuarentena afecta operaciones puntuales y el bloqueo TRL afecta el archivo completo.

### 4.4 Hallazgos exploratorios y decisiones


| Fuente / hallazgo | Evidencia | Decisión |
|---|---:|---|
| CRM: duplicados | 326 registros involucrados (163 grupos) | Deduplicar en Silver por `pais + comercio_id + snapshot_date`; conservar snapshots Bronze |
| CRM: espacios en nombre | 539 | `TRIM` en Silver |
| CRM: espacios en ciudad | 37 | `TRIM` en Silver |
| CRM: formatos de fecha de afiliación | 3 formatos / 17.558 registros | Normalizar a `DATE` |
| CRM: campos críticos vacíos | 0 | No excluir por este criterio |
| Productos: SKU nulo | 18 (60 %) | No exigir SKU como llave; utilizar país + producto |
| Productos: `vigente_hasta` nulo | 29 (96,67 %) | Interpretar como vigencia abierta |
| Productos: líneas origen | 12 etiquetas frente a 7 corporativas | Homologar en Silver y conservar original |
| Canales y taxonomía | Sin vacíos ni duplicados reportados | Mantener catálogo y homologar por país + código |
| México: ID_OPERACION repetido | 198 filas adicionales en inventario | Conservar en Bronze; investigar y resolver versión en Silver |
| México: MONTO vacío | 25 en inventario | Cuarentena; no convertir en cero |
| México: variantes de canal | 7 representaciones | Normalizar mayúsculas/minúsculas y homologar |
| México: CANCELADA | 202 | Estado válido; homologar como `REVERSADA` |
| Colombia: operaciones CDC | 39.330 `c`, 8.175 `u`, 7 `d` | Aplicar secuencia CDC; `d` elimina estado vigente |
| Colombia: `op` nulo | 7 | Revisar como tombstones; no son transacciones |
| Colombia: documento_cliente nulo | 34.642 | Conservar nulo; no inventar documentos |
| Colombia: referencia nula | 15.255 | Conservar; obligatoriedad por negocio pendiente |
| Colombia: comisión nula | 136 | Conservar y validar obligatoriedad en Silver |

### 4.5 Calidad adicional de la consolidación

`sql/silver/03_consolidar_transacciones.sql` evalúa estado, fecha, monto, comisión, canal, producto, línea corporativa y rango permitido por producto. También identifica el producto de prueba `TST-001` de Colombia.

La ejecución documentada reporta las observaciones prioritarias:

| Observación prioritaria | Registros |
|---|---:|
| `COMISION_INVALIDA` | 251 |
| `MONTO_FUERA_DE_RANGO_PRODUCTO` | 28 |
| `PRODUCTO_NO_HOMOLOGADO` | 139 |
| Sin observación prioritaria | 131.724 |

Se asigna una sola `observacion_calidad` prioritaria por registro mediante un `CASE`; por ello estos conteos no representan necesariamente todas las anomalías simultáneas. Los indicadores booleanos permiten inspeccionar otras condiciones.

**Producto de prueba:** `TST-001` se marca con `es_producto_prueba`. La definición de negocio indica que comercios y productos de prueba no forman parte de cifras oficiales. Debe verificarse el filtro en Gold/consultas antes de publicar cifras. La marca en Silver por sí sola no garantiza exclusión.

## 5. Transformaciones por país

### 5.1 Colombia – CDC Debezium/Kafka

La fuente entrega JSON Lines con operaciones `c`, `u` y `d`. Bronze conserva el evento completo, incluyendo `before`, `after`, `op` y metadatos Kafka. No se deduplica por `trx_id` en Bronze.

En `01_transacciones_colombia.sql`:

1. Se obtiene la llave con `COALESCE(after.trx_id, before.trx_id, key.trx_id)`.
2. Se filtran operaciones `c`, `u`, `d`.
3. Se deduplican mensajes repetidos por `topic + partition + offset`.
4. Se selecciona el último evento por transacción con `ROW_NUMBER()`, ordenando primero por `value.source.lsn`, luego `value.ts_ms`, `kafka_ts` y `offset`.
5. Si el último evento es `d`, o no tiene `after`, no queda como transacción vigente.
6. Se tipifican monto y comisión a `DECIMAL(18,2)` y fecha de negocio desde `yyyyMMddHHmmss`.

**Justificación del LSN:** identifica la posición del cambio en el log de PostgreSQL. `kafka_ts` puede reflejar retrasos; `offset` solo ordena dentro de una partición; `updated_at` no necesariamente está presente ni constituye un orden CDC global. Los demás campos actúan como desempate.

Se conservan nulos de `documento_cliente` y `referencia`; no se inventan datos ni se descartan transacciones por esos atributos sin una regla de negocio que lo exija. Los eventos tombstone (`op` nulo) se tratan como control y no como transacciones; el SQL de Silver filtra `c/u/d`.

### 5.2 México – CSV y TRL

Los 11 campos originales se mantienen como texto en Bronze, incluyendo el registro TRL. En Silver se selecciona la última versión por `ID_OPERACION` con `ROW_NUMBER()`, usando fecha de actualización y metadatos de ingesta como orden. Los estados se homologan; `CANCELADA` se mapea a `REVERSADA`.

El inventario identificó 198 filas adicionales repetidas por `ID_OPERACION`; el informe final de Silver registra 408 operaciones repetidas que no se conservaron como filas adicionales. Son resultados de análisis/corridas diferentes y no se deben sumar. Para los resultados finales consolidados se utiliza el resultado de ejecución de Silver, que informa 51.860 transacciones México y cero llaves duplicadas.

## 6. CRM y catálogos

### 6.1 CRM

El CRM llega como snapshots completos. Se conserva cada fotografía en Bronze con `snapshot_date` y metadatos. No se sobrescriben todos los snapshots por `comercio_id`, porque se perdería evidencia histórica.

En Silver se normalizan textos y fechas, se deduplica por país, comercio y fecha de snapshot, y se construye una salida vigente para el enriquecimiento de consultas. Los estados `ACTIVO`, `INACTIVO` y `SUSPENDIDO` son estados válidos, no anomalías.

**Limitación:** el flujo procesa `ola_1` y `ola_2` mediante una lista fija en el código; no es dinámico por parámetro. Además, el modelo entregado publica CRM vigente, pero no constituye una dimensión SCD Tipo 2 completa con intervalos `valid_from` / `valid_to` para reconstruir atributos históricos al momento de la transacción.

### 6.2 Productos

Se conserva el JSON de origen, incluidos objetos anidados como `comision` y `limites`. `sku_origen` puede ser nulo y no es llave obligatoria; la identificación se apoya en país + `producto_cod`. `vigente_hasta` nulo se interpreta como vigencia abierta. Las etiquetas originales de línea se homologan contra la taxonomía corporativa en Silver.

### 6.3 Canales y taxonomía

Se homologan canales con la llave compuesta país + código de origen. Ejemplos documentados: CO `1 → POS`, `2 → APP`, `3 → WEB`, `4 → API`; MX `TPV → POS`, `APP → APP`, `WS → API`. La taxonomía de siete líneas corporativas sirve como catálogo de referencia para las etiquetas de producto.

## 7. Definiciones de negocio y métricas

Las notas funcionales definen:

- **Fecha de negocio:** fecha local de la transacción en su país; Colombia `America/Bogota`, México `America/Mexico_City`.
- **Transacción efectiva:** estado final `APROBADA`.
- **Reverso/cancelación:** un reverso posterior (`REVERSADA` / `CANCELADA`) anula la transacción efectiva en su fecha de negocio original.
- **Ingreso:** comisión de la transacción efectiva, en moneda local.
- **Prueba:** transacciones de comercios o productos de prueba no forman parte de las cifras oficiales.
- **Rango de producto:** montos fuera de los límites del catálogo no son válidos para cifras oficiales.

Los montos se mantienen en moneda local (`COP` o `MXN`) y no se agregan entre países sin conversión cambiaria explícita.

**Control de alineación requerido:** la lógica de Silver conserva el estado final por transacción y mapea `CANCELADA` a `REVERSADA`. Antes de certificar Gold, validar con casos de prueba que un reverso tardío retire la transacción del indicador efectivo y que el ajuste se refleje en la fecha original de negocio. Verificar igualmente que Gold excluya `es_producto_prueba`, `observacion_calidad` y montos fuera de rango. La definición funcional es vinculante, pero los filtros deben confirmarse en el SQL de Gold.

## 8. Resultados de la ejecución documentada

La documentación de transformaciones Silver registra:

| Indicador | Resultado |
|---|---:|
| Colombia | 80.282 |
| México | 51.860 |
| Total consolidado | 132.142 |
| Llaves nulas CO / MX | 0 / 0 |
| Llaves duplicadas CO / MX | 0 / 0 |
| Estados nulos | 0 |
| Canales sin homologación | 0 |

Distribución por estado:

| País | APROBADA | RECHAZADA | PENDIENTE | REVERSADA |
|---|---:|---:|---:|---:|
| CO | 70.035 | 9.363 | 85 | 799 |
| MX | 45.749 | 4.668 | 1.015 | 428 |

La tabla consolidada se persiste en `data/lakehouse/silver/transacciones_sql`. Los valores son evidencia de esa corrida y pueden cambiar con nuevas cargas o reglas.

## 9. Respuestas a las preguntas de criterio de la prueba

### Nivel 1 – Fundamentos

**¿Qué problema de calidad impacta más Finanzas y cómo se cuantifica?**  
Los montos nulos/no interpretables afectan el valor e ingreso; las diferencias TRL pueden indicar archivos incompletos y comprometer todos sus registros; los estados, fechas y homologaciones incorrectas distorsionan conteos y segmentaciones. Se cuantifica con cuarentena por registro, diferencia de conteo/suma por archivo, hallazgos y conteos por observación Silver. El caso TRL bloqueado tuvo diferencia de 223 operaciones y 111.628,00. No se debe presentar el número de cuarentena de un inventario exploratorio como si fuera el de la corrida final.

**¿Cómo se definió el estado vigente por país y cuándo podría fallar?**  
Colombia selecciona el último evento CDC por transacción, priorizando LSN; un evento final `d` elimina el estado vigente. México selecciona la última versión por `ID_OPERACION`, ordenando por fecha de actualización y metadatos de carga. Puede fallar si falta el campo de orden, hay empates, identificadores reutilizados o los archivos no reflejan el estado definitivo.

**¿Qué se hizo con registros no interpretables?**  
Se conservan en Bronze. Los montos nulos/no interpretables se identifican en Quality y se envían a cuarentena; Silver los excluye mediante la llave de archivo + operación. No se reemplazan por cero. Los archivos bloqueados por TRL no pasan a Silver.

**¿Qué tipos se eligieron para montos y fechas?**  
`DECIMAL(18,2)` para importes y comisiones. Las fechas se convierten desde formatos locales a timestamp/date, manteniendo la fecha de negocio local. Se evita la imprecisión de flotantes y confundir fecha de ingesta con fecha de transacción.

### Nivel 2 – Pipeline confiable

**¿Por qué incrementalidad por archivo y qué no resuelve?**  
Checksum + ruta en Colombia y `source_file` en México permiten reejecución controlada y trazabilidad para las olas. No es un MERGE de filas. Una corrección de archivo con la misma ruta en México requiere una estrategia explícita de reemplazo/reproceso.

**¿Qué campo ordena CDC y por qué no kafka_ts, offset o updated_at?**  
El principal es `value.source.lsn`, posición del cambio en el log de PostgreSQL. `kafka_ts` puede reflejar retraso; `offset` solo ordena dentro de partición; `updated_at` no necesariamente está disponible ni ofrece orden total. Se usan los demás campos como desempate.

**¿Qué reglas bloquean publicación y quién debería decidirlo?**  
La inconsistencia TRL bloquea un archivo completo; montos nulos/no interpretables generan cuarentena individual. En Silver se asignan observaciones de estado, fecha, monto, comisión, canal, producto, línea y rango. Finanzas debe aprobar el impacto en métricas; Riesgo define tolerancias; Gobierno de Datos y dueños de fuente mantienen homologaciones; Ingeniería implementa controles y monitoreo.

**¿Cuál es el grano de Gold y qué preguntas no responde?**  
El grano documentado es fecha de negocio + país + línea corporativa. Sirve para métricas diarias agregadas, pero no para identificar por sí solo comercio, producto, canal o transacción. Esas preguntas requieren Silver o una Gold de mayor granularidad.

### Nivel 3 – Avanzado

**¿Cómo evita SCD2 versiones falsas y qué columnas detecta?**  
La entrega conserva snapshots CRM y publica un maestro vigente, pero no demuestra un SCD2 completo con intervalos de vigencia. Por tanto, no se afirma que implemente detección histórica de cambios sin versiones falsas. Para completarlo habría que comparar atributos de negocio (segmento, ciudad, región, estado, plan de comisión), excluir metadatos de ingesta y versionar solo cuando esos atributos cambien.

**Llega un reverso de hace nueve días: ¿qué recalcula?**  
El estado vigente CDC puede cambiar al reprocesar Silver y el modelo debe retirar el importe efectivo en la fecha original de negocio. La ejecución actual regenera Silver y Gold desde los datos disponibles; no se evidencia recálculo selectivo solo de las particiones afectadas. La actualización funcional debe probarse con un caso tardío.

**¿Qué ocurre si una transacción llega antes que su comercio en CRM?**  
La transacción debe permanecer en Silver; el enriquecimiento CRM puede quedar nulo hasta que llegue el snapshot. Se recomienda registrar el caso como comercio sin maestro y enriquecerlo posteriormente. El SCD2 histórico para resolver atributos vigentes al momento exacto de la transacción queda pendiente.

## 10. Trade-offs y escenarios de evolución

- **Reversos con latencia menor a cinco minutos:** pasar a CDC en streaming/micro-batches y actualizar estado por llave en Delta; mejora latencia, pero eleva complejidad y costo.
- **Volumen 50 veces mayor:** revisar lecturas repetidas, `collect()`, `toPandas()` y shuffles; adoptar ingesta administrada, procesamiento distribuido, particionamiento, optimización Delta y orquestación.
- **Segregación por país/datos personales:** Unity Catalog con permisos por catálogo/esquema/tabla, vistas seguras, políticas de filtrado/mascaramiento, grupos separados y auditoría.
- **Corrupción de tres días de Silver:** preservar la versión previa, reconstruir desde Bronze, reprocesar el rango, validar calidad/conciliación y regenerar Gold afectada.

## 11. Limitaciones y próximos pasos

1. Unificar la ejecución en un orquestador que coordine olas y fuentes.
2. Parametrizar CRM y catálogos; hoy CRM tiene lista fija de olas.
3. Implementar MERGE transaccional real donde aplique y definir reemplazo de archivos corregidos.
4. Centralizar reglas, severidades, umbrales y responsables de calidad.
5. Completar SCD2 para CRM.
6. Validar y automatizar el tratamiento de reversos tardíos sobre fecha original.
7. Confirmar que Gold excluya productos/comercios de prueba y registros fuera de rango, según definición de negocio.
8. Incorporar pruebas automatizadas reproducibles y conciliaciones de salida.
9. Hacer que los scripts consuman efectivamente los YAML de configuración.
10. Agregar bitácora de corridas, alertas y recuperación operativa.

## 12. Cierre

La solución cubre el flujo principal Landing → Bronze → Quality → Silver → Gold y la generación de cuatro consultas SQL. Sus principales fortalezas son la conservación de origen, control de reingesta por archivo, selección de estado CDC, control TRL, cuarentena individual, bloqueo por archivo, homologación y conciliación de Silver. Las limitaciones de SCD2, parametrización CRM, MERGE de filas y recálculo incremental quedan explícitas como trabajo pendiente.
