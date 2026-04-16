# motor_ingesta

Librería Python para la ingesta y procesamiento diario de datos de vuelos en formato JSON hacia tablas Spark/Parquet, con soporte para ejecución local y en Databricks.

---

## Estructura del proyecto

```
motor_ingesta/
├── __init__.py
├── motor_ingesta.py       # Clase MotorIngesta: lectura y aplanado de JSON
├── flujo_diario.py        # Clase FlujoDiario: orquestación del pipeline diario
├── agregaciones.py        # Funciones de enriquecimiento (UTC, intervalos)
└── resources/
    └── timezones.csv      # Tabla de zonas horarias por código IATA

tests/ # Tests del proyecto (pytest)
config/
└── config.json # Configuración de ejemplo
```

---

## Módulos

### `MotorIngesta`

Clase principal de ingesta. Lee un fichero JSON desde DBFS o ruta local, aplana recursivamente su estructura (explota arrays y desanida structs a cualquier nivel de profundidad) y selecciona únicamente las columnas declaradas en la configuración, casteándolas al tipo indicado y añadiendo comentarios como metadatos.

**Uso básico:**

```python
from motor_ingesta.motor_ingesta import MotorIngesta

config = {
    "data_columns": [
        {"name": "FlightDate", "type": "date",    "comment": "Fecha del vuelo"},
        {"name": "Origin",     "type": "string",  "comment": "Aeropuerto de origen"},
        {"name": "DepTime",    "type": "integer", "comment": "Hora de salida local (HHMM)"},
    ]
}

motor = MotorIngesta(config, spark=spark)
df = motor.ingesta_fichero("/dbfs/mnt/raw/flights/2023-01-01.json")
```

Acepta un parámetro `spark` opcional para inyectar una `SparkSession` existente (útil con `DatabricksSession`). Si no se pasa, obtiene o crea una sesión local automáticamente.

---

### `FlujoDiario`

Orquestador del pipeline diario. Lee la configuración desde un fichero JSON, instancia la sesión Spark adecuada (local o Databricks) y ejecuta el flujo completo de procesamiento para un fichero de datos.

**Pasos del flujo (`procesa_diario`):**

1. **Ingesta** del fichero JSON mediante `MotorIngesta`.
2. **Conversión a UTC** de la hora de salida (`aniade_hora_utc`).
3. **Unión con el día previo**: si existe la partición del día anterior en la tabla de salida, la incorpora para calcular correctamente los intervalos entre días.
4. **Cálculo de intervalos** entre vuelos consecutivos por aeropuerto (`aniade_intervalos_por_aeropuerto`).
5. **Escritura** en tabla Parquet particionada por `FlightDate`, con sobreescritura dinámica de particiones.

**Uso básico:**

```python
from motor_ingesta.flujo_diario import FlujoDiario

flujo = FlujoDiario("config/config.json")
flujo.procesa_diario("data/flights_json/landing/2023-01-01.json")
```

---

### `agregaciones`

Funciones utilitarias de enriquecimiento del DataFrame de vuelos.

#### `aniade_hora_utc(spark, df)`

Añade la columna `FlightTime` (timestamp UTC) a partir de `FlightDate`, `DepTime` y la zona horaria del aeropuerto de origen (`Origin`), consultando el CSV de timezones incluido en `resources/`.

#### `aniade_intervalos_por_aeropuerto(df)`

Añade tres columnas calculadas mediante una ventana (`Window`) particionada por aeropuerto de origen y ordenada por `FlightTime`:

| Columna | Tipo | Descripción |
|---|---|---|
| `FlightTime_next` | timestamp | Hora UTC de despegue del siguiente vuelo en el mismo aeropuerto |
| `Airline_next` | string | Aerolínea del siguiente vuelo |
| `diff_next` | long | Diferencia en segundos entre el vuelo siguiente y el actual |

---

## Configuración

La configuración se pasa como un diccionario (para `MotorIngesta`) o como ruta a un fichero JSON (para `FlujoDiario`).

**Ejemplo de `config.json`:**

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "CLUSTER_ID": "",
  "output_table": "default.flights_processed",
  "output_partitions": 4,
  "data_columns": [
    {"name": "FlightDate",        "type": "date",    "comment": "Fecha del vuelo"},
    {"name": "Reporting_Airline", "type": "string",  "comment": "IATA de la aerolínea"},
    {"name": "Origin",            "type": "string",  "comment": "Aeropuerto de origen"},
    {"name": "Dest",              "type": "string",  "comment": "Aeropuerto de destino"},
    {"name": "DepTime",           "type": "integer", "comment": "Hora de salida local (HHMM)"}
  ]
}
```

| Parámetro | Descripción |
|---|---|
| `EXECUTION_ENVIRONMENT` | `"databricks"` para conectar a un cluster remoto; cualquier otro valor para sesión local |
| `CLUSTER_ID` | ID del cluster Databricks (solo necesario si el entorno es `"databricks"`) |
| `output_table` | Nombre de la tabla Hive/Delta donde se escribe el resultado |
| `output_partitions` | Número de particiones de salida |
| `data_columns` | Lista de columnas a seleccionar, con nombre, tipo Spark y comentario |

---
## Entornos virtuales

### Local (PySpark)

```
python -m venv venv_local
source venv_local/bin/activate
pip install pyspark pandas loguru pytest
```

### Databricks

```
python -m venv venv_databricks
source venv_databricks/bin/activate
pip install databricks-connect pyspark pandas loguru pytest
```

---

## Autenticación con Databricks

Usar token (PAT):

```
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<token>
export DATABRICKS_CLUSTER_ID=<cluster-id>
```

Validar:

```
databricks-connect test
```

---

## Testing

```
pytest tests/
```

Verbose:

```
pytest tests/ -v
```

```bash
(.venv-linux) brrsanchezfi@brrsanchezfi:/mnt/c/Users/braya/OneDrive/Escritorio/spark_nticmaster$ pytest tests/
================================================================================ test session starts ================================================================================
platform linux -- Python 3.10.12, pytest-9.0.3, pluggy-1.6.0
rootdir: /mnt/c/Users/braya/OneDrive/Escritorio/spark_nticmaster
plugins: anyio-4.13.0
collected 4 items

tests/test_ingesta.py ....                                                                                                                                                    [100%]

================================================================================ 4 passed in 37.75s =================================================================================
(.venv-linux) brrsanchezfi@brrsanchezfi:/mnt/c/Users/braya/OneDrive/Escritorio/spark_nticmaster$ 
```


---

## Construcción del paquete (.whl)

Instalar:

```
pip install build
```

Construir:

```
python -m build
```

Salida:

```
dist/
├── motor_ingesta-<version>.tar.gz
└── motor_ingesta-<version>-py3-none-any.whl
```

Instalar:

```
pip install dist/motor_ingesta-<version>-py3-none-any.whl
```

---


## Dependencias

- `pyspark`
- `pandas`
- `loguru`
- `databricks-connect` *(solo si `EXECUTION_ENVIRONMENT` es `"databricks"`)*

---

## Notas

- El aplanado de `MotorIngesta` asume que los nombres de campos anidados son únicos entre todos los niveles del JSON.
- El fichero `resources/timezones.csv` debe contener al menos las columnas `iata_code` e `iana_tz`. Los vuelos cuyo aeropuerto no figure en el CSV mantienen `FlightTime` como `null`.
- La escritura en tabla usa sobreescritura dinámica de particiones (`partitionOverwriteMode = dynamic`), de modo que solo se reemplazan las particiones procesadas en cada ejecución.














---

