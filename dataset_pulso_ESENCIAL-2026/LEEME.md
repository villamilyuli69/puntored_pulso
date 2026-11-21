# Paquete de datos - Proyecto Pulso · Version Esencial (Puntored · Data & Analytics)

Paquete: ESENCIAL-2026

Este paquete simula la *landing zone* en S3 (`s3://ptr-datalake-landing-prd/`). Los datos son sinteticos,
pero su estructura y comportamiento replican sistemas reales. La documentacion de cada fuente la entregaron
los equipos duenos de esos sistemas y **puede estar incompleta o desactualizada**: validala contra los datos.

## Olas de entrega
Procesa cada ola como una corrida independiente del pipeline, en orden: `ola_1` -> `ola_2`.

| Ola | Contenido (por fecha de llegada a la landing) |
|-----|------------------------------------------------|
| ola_1 | Archivos que llegaron entre 2026-06-01 y 2026-06-30 |
| ola_2 | Archivos que llegaron entre 2026-07-01 y 2026-08-02 |

## Fuentes
| Ruta | Sistema fuente | Formato | Documentacion del equipo fuente |
|------|----------------|---------|---------------------------------|
| `landing/co/core_transacciones_cdc/fecha_carga=YYYY-MM-DD/*.jsonl` | Core transaccional Colombia (PostgreSQL) via Debezium -> Kafka -> conector S3 | JSON Lines | Envoltorio: `key`, `value` (evento Debezium con `before`, `after`, `source`, `op`, `ts_ms`), `topic`, `partition`, `offset`, `kafka_ts`. `op`: c=insert, u=update, d=delete. `after.fecha_trx` es numerico `yyyyMMddHHmmss` en hora de Colombia. `valor` y `comision` como texto decimal. |
| `landing/mx/plataforma_transacciones/fecha_carga=YYYY-MM-DD/trx_mx_YYYYMMDD.csv` | Plataforma transaccional Mexico | CSV `;` con registro de control final `TRL;conteo;suma_monto` | Un archivo por dia de operacion con el estado de cada operacion al momento de la extraccion. `FECHA_HORA` en formato `dd/MM/yyyy HH:mm:ss`, hora de Ciudad de Mexico. `ID_COMERCIO` con formato `MX-NNNNNN`. |
| `landing/crm/comercios/snapshot_fecha=YYYY-MM-DD/comercios.csv` | CRM comercial | CSV `,` | Foto completa semanal (lunes) del maestro de comercios de ambos paises. |
| `landing/catalogos/productos/version=YYYY-MM-DD/productos.json` | Gerencia de Producto | JSON | Catalogo de productos CO y MX con linea de negocio, comision y limites de monto. |
| `landing/catalogos/canales/canales.csv` | Gerencia de Producto | CSV | Homologacion de canales por pais. |
| `landing/catalogos/taxonomia/lineas_negocio_corporativas.csv` | Gobierno de Datos | CSV | Lineas de negocio corporativas oficiales. |

## Definiciones de negocio (vigentes)
- **Fecha de negocio**: fecha local de la transaccion en su pais (CO: America/Bogota, MX: America/Mexico_City).
- **Transaccion efectiva**: estado final APROBADA. Un reverso posterior (REVERSADA/CANCELADA) anula la transaccion en su fecha de negocio original.
- **Ingreso**: comision de la transaccion efectiva en moneda local.
- Transacciones de comercios o productos de prueba **no** hacen parte de las cifras oficiales.
- Montos por fuera de los limites del catalogo del producto no son validos para cifras oficiales.
