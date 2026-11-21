Fuente	Hallazgo	Magnitud	Decisión
CRM	Duplicados por snapshot	326 (1,86 %)	Deduplicar en Silver
CRM	Espacios en nombre comercial	539 (3,07 %)	Aplicar TRIM
CRM	Espacios en ciudad	37 (0,21 %)	Aplicar TRIM
CRM	Formatos diferentes de fecha de afiliación	3 formatos / 17.558 registros	Normalizar a DATE
CRM	Campos críticos vacíos	0	No requiere exclusión
Productos	SKU nulo	18 (60 %)	No usar como llave obligatoria
Productos	Vigente_hasta nulo	29 (96,67 %)	Interpretar como posible vigencia abierta
Productos	Etiquetas de línea de negocio	12 frente a 7 corporativas	Homologar en Silver
Productos	Llaves duplicadas	0	Sin corrección necesaria
Canales	Campos vacíos	0	Sin corrección necesaria
Canales	Llaves duplicadas	0	Sin corrección necesaria
Taxonomía	Campos vacíos	0	Sin corrección necesaria
Taxonomía	Líneas duplicadas	0	Sin corrección necesaria
Mexico	ID_OPERACION repetidos	198 filas adicionales	0,81%	Conservar en Bronze. Investigar la duplicidad en Silver antes de eliminar registros.
Mexico	MONTO vacío	25	0,10%	Conservar el registro original y enviarlo a revisión o cuarentena en Silver. No convertirlo automáticamente en cero.
Mexico	Variantes de CANAL	7 representaciones	—	Normalizar mayúsculas/minúsculas y homologar con el catálogo.
Mexico	Estado CANCELADA	202	0,82%	Conservarlo como estado de negocio y homologarlo con la lógica de reversos/cancelaciones.
Colombia  Eventos de creación (c)	39.330	82,77%	Aplicar como creación de la transacción.
Colombia  Eventos de actualización (u)	8.175	17,20%	Aplicar como actualización, respetando el orden CDC.
Colombia  Eventos de eliminación (d)	7	0,015%	Aplicar como eliminación lógica del estado vigente.
Colombia  Eventos sin operación (op nulo)	7	0,015%	Revisar como eventos tombstone asociados a las eliminaciones; no tratarlos como transacciones.
Colombia  Eventos con documento_cliente nulo	34.642	72,90%	Conservar el nulo; no inventar documentos.
Colombia  Eventos con referencia nula	15.255	32,10%	Conservar el nulo; revisar si el campo es opcional según el negocio.
Colombia  Eventos con comision nula	136	0,29%	Conservar el dato y validar su obligatoriedad en Silver.